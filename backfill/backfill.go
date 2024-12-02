package backfill

import (
	"bsky/storage"
	"bsky/storage/models"
	"bsky/utils"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/araddon/dateparse"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ipfs/go-cid"
	log "github.com/sirupsen/logrus"
	"github.com/whyrusleeping/cbor-gen"
	"hash"
	"hash/fnv"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	AccountDeactivatedError = "AccountDeactivated"
	InvalidRequestError     = "InvalidRequest" // Seen when profile is not found
)

type RepoMeta struct {
	Data   *comatproto.SyncListRepos_Repo
	PdsUrl string
}

type Backfiller struct {
	serviceName        string
	numRepoWorkers     int
	numMetadataWorkers int
	storageManager     *storage.Manager
	languageDetector   *utils.LanguageDetector
	hasher             hash.Hash32
}

func NewBackfiller(
	serviceName string,
	storageManager *storage.Manager,
	numRepoWorkers int,
	numMetadataWorkers int,
) *Backfiller {
	return &Backfiller{
		serviceName:        serviceName,
		numRepoWorkers:     numRepoWorkers,
		numMetadataWorkers: numMetadataWorkers,
		storageManager:     storageManager,
		languageDetector:   utils.NewLanguageDetector(),
		hasher:             fnv.New32a(),
	}
}

func (b *Backfiller) Run() {
	now := time.Now()

	// Span workers
	wgRepo := sync.WaitGroup{}
	repoChan := make(chan *RepoMeta)

	for i := 0; i < b.numRepoWorkers; i++ {
		wgRepo.Add(1)
		go b.repoWorker(&wgRepo, repoChan)
	}

	cursor := b.storageManager.GetCursor(b.serviceName)
	for {
		response, err := http.Get(
			fmt.Sprintf("https://plc.directory/export?count=1000&after=%s", cursor),
		)
		if err != nil {
			log.Errorf("PLC export failed: %v", err)
			time.Sleep(30 * time.Second)
			continue
		}

		scanner := bufio.NewScanner(response.Body)

		// Read each line from the response
		for scanner.Scan() {
			line := scanner.Bytes()

			var plcEntry PlcLogEntry
			err = json.Unmarshal(line, &plcEntry)
			if err != nil {
				log.Errorf("Invalid PLC export content format: %v", err)
				continue
			}

			// Update user metadata
			createdAt, err := dateparse.ParseAny(plcEntry.CreatedAt)
			if err != nil {
				log.Errorf("Unable to parse time: %v", err)
				break
			}
			handle := ""
			if len(plcEntry.Operation.AlsoKnownAs) > 0 {
				handle = strings.Replace(plcEntry.Operation.AlsoKnownAs[0], "at://", "", -1)
			}
			_, err = b.storageManager.GetOrCreateUser(plcEntry.Did)
			if err == nil {
				b.storageManager.SetUserMetadata(
					plcEntry.Did,
					handle,
					createdAt,
				)
			}

			// Process corresponding PDS
			services := plcEntry.Operation.Services
			if services != nil {
				b.processPds(services.AtProtoPds.Endpoint, repoChan)
			}

			// Update cursor
			cursor = plcEntry.CreatedAt
			b.storageManager.UpdateCursor(b.serviceName, cursor)
		}

		// Check for errors that may have occurred during scanning
		if err := scanner.Err(); err != nil {
			log.Errorf("Error reading response: %v", err)
		}

		cursorTime, err := dateparse.ParseAny(cursor)
		if err != nil {
			log.Errorf("Unable to parse cursor time: %v", err)
			break
		}
		if now.Before(cursorTime) {
			// All entries prior to start have been processed. Sync finished
			break
		}

		_ = response.Body.Close()
	}

	close(repoChan)
	wgRepo.Wait()
}

func (b *Backfiller) handleFollowCreate(did string, uri string, follow *appbsky.GraphFollow) {
	uriParts := strings.Split(uri, "/")
	uriKey := uriParts[len(uriParts)-1]

	createdAt, err := dateparse.ParseAny(follow.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return
	}

	authorId, err := b.storageManager.GetOrCreateUser(did)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return
	}
	subjectId, err := b.storageManager.GetOrCreateUser(follow.Subject)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return
	}
	b.storageManager.CreateFollow(
		models.Follow{
			UriKey:    uriKey,
			AuthorID:  authorId,
			SubjectID: subjectId,
			CreatedAt: createdAt,
		},
	)
}

func (b *Backfiller) handleInteractionCreate(
	did string,
	uri string,
	kind models.InteractionType,
	createdAtStr string,
	subjectUri string,
) {
	if !strings.Contains(subjectUri, "/app.bsky.feed.post/") {
		// Likes can be given to feeds too
		return
	}
	uriParts := strings.Split(uri, "/")
	uriKey := uriParts[len(uriParts)-1]

	createdAt, err := dateparse.ParseAny(createdAtStr)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return
	}

	authorId, err := b.storageManager.GetOrCreateUser(did)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return
	}
	postAuthorDid, postUriKey, err := utils.SplitUri(subjectUri, "/app.bsky.feed.post/")
	if err != nil {
		log.Errorf("Error parsing post uri: %v", err)
		return
	}
	postAuthorId, err := b.storageManager.GetOrCreateUser(postAuthorDid)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return
	}
	postId, err := b.storageManager.GetPostId(postAuthorId, postUriKey)
	if err != nil {
		log.Errorf("Error getting post id: %v", err)
		return
	}
	b.storageManager.CreateInteraction(
		models.Interaction{
			UriKey:    uriKey,
			Kind:      kind,
			AuthorId:  authorId,
			PostId:    postId,
			CreatedAt: createdAt,
		},
	)
}

func (b *Backfiller) handlePostCreate(did string, uri string, post *appbsky.FeedPost) {
	createdAt, err := dateparse.ParseAny(post.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return
	}

	uriParts := strings.Split(uri, "/")
	uriKey := uriParts[len(uriParts)-1]

	var replyParentId, replyRootId int64
	if post.Reply != nil {
		if post.Reply.Parent != nil {
			authorDid, uriKey, err := utils.SplitUri(post.Reply.Parent.Uri, "/app.bsky.feed.post/")
			if err != nil {
				return
			}
			authorId, err := b.storageManager.GetOrCreateUser(authorDid)
			if err != nil {
				return
			}
			replyParentId, err = b.storageManager.GetPostId(authorId, uriKey)
			if err != nil {
				return
			}
		}
		if post.Reply.Root != nil {
			authorDid, uriKey, err := utils.SplitUri(post.Reply.Root.Uri, "/app.bsky.feed.post/")
			if err != nil {
				return
			}
			authorId, err := b.storageManager.GetOrCreateUser(authorDid)
			if err != nil {
				return
			}
			replyRootId, err = b.storageManager.GetPostId(authorId, uriKey)
			if err != nil {
				return
			}
		}
	}

	language := b.languageDetector.DetectLanguage(post.Text, post.Langs)

	// Calculate rank
	b.hasher.Write([]byte(uri))
	hash := b.hasher.Sum32()
	b.hasher.Reset()
	decimalPlaces := int(math.Log10(float64(hash))) + 1
	divisor := math.Pow10(decimalPlaces)
	rank := float64(createdAt.Unix()) + float64(hash)/divisor

	authorId, err := b.storageManager.GetOrCreateUser(did)
	if err != nil {
		log.Errorf("Error creating user: %v", err)
		return
	}
	b.storageManager.CreatePost(
		models.Post{
			UriKey:        uriKey,
			AuthorId:      authorId,
			AuthorDid:     did,
			ReplyParentId: replyParentId,
			ReplyRootId:   replyRootId,
			CreatedAt:     createdAt,
			Language:      language,
			Rank:          rank,
			Text:          post.Text,
			Embed:         post.Embed,
		})
}

func (b *Backfiller) processBskyError(did string, err error, callback func()) {
	retry := false
	var bskyErr *xrpc.Error

	if errors.As(err, &bskyErr) {
		if bskyErr.StatusCode == 400 {
			var wrappedError *xrpc.XRPCError

			if errors.As(bskyErr.Wrapped, &wrappedError) {
				switch wrappedError.ErrStr {
				case AccountDeactivatedError, InvalidRequestError:
					// Delete user if profile does not exist anymore
					b.storageManager.DeleteUser(did)
				}
			}
		}

		// Sleep if API rate limit has been exceeded
		if bskyErr.Ratelimit != nil && bskyErr.Ratelimit.Remaining == 0 {
			time.Sleep(bskyErr.Ratelimit.Reset.Sub(time.Now()))
			retry = true
		}
	}

	if retry {
		callback()
	}
}

func (b *Backfiller) processPds(url string, repoChan chan *RepoMeta) {
	ctx := context.Background()
	cursorFinished := "done"

	if url == "https://bsky.social" {
		// Skip all single-PDS
		return
	}

	cursor := b.storageManager.GetCursor(url)
	if cursor == cursorFinished {
		return
	}

	xrpcClient := &xrpc.Client{
		Host:   url,
		Client: http.DefaultClient,
	}

	attempts := 0
	for {
		response, err := comatproto.SyncListRepos(ctx, xrpcClient, cursor, 1000)
		if err != nil {
			log.Errorf("SyncListRepos failed: %v", err)
			time.Sleep(30 * time.Second)
			attempts++
			if attempts < 3 {
				continue
			} else {
				log.Errorf("Skipped SyncListRepos for '%s'", url)
				break
			}
		}
		attempts = 0

		for _, repoDescription := range response.Repos {
			if repoDescription.Active == nil || !*repoDescription.Active {
				continue
			}
			repoChan <- &RepoMeta{
				Data:   repoDescription,
				PdsUrl: url,
			}
		}

		if response.Cursor == nil {
			break
		}
		cursor = *response.Cursor
		b.storageManager.UpdateCursor(url, cursor)
	}

	b.storageManager.UpdateCursor(url, cursorFinished)
}

func (b *Backfiller) processRecord(repoDid string, uri string, record typegen.CBORMarshaler) {
	switch data := record.(type) {
	case *appbsky.FeedPost:
		b.handlePostCreate(
			repoDid, uri, data,
		)
	case *appbsky.FeedLike:
		b.handleInteractionCreate(
			repoDid,
			uri,
			models.Like,
			data.CreatedAt,
			data.Subject.Uri,
		)
	case *appbsky.FeedRepost:
		b.handleInteractionCreate(
			repoDid,
			uri,
			models.Repost,
			data.CreatedAt,
			data.Subject.Uri,
		)
	case *appbsky.GraphFollow:
		b.handleFollowCreate(
			repoDid, uri, data,
		)
	}
}

func (b *Backfiller) processRepo(repoMeta *RepoMeta) {
	ctx := context.Background()
	did := repoMeta.Data.Did
	log.Warnf("Backfilling '%s'", did)

	xrpcClient := &xrpc.Client{
		Host:   repoMeta.PdsUrl,
		Client: http.DefaultClient,
	}

	repoBytes, err := comatproto.SyncGetRepo(ctx, xrpcClient, did, "")
	if err != nil {
		log.Errorf("SyncGetRepo failed for did '%s': %v", did, err)
		b.processBskyError(did, err, func() { b.processRepo(repoMeta) })
		return
	}

	repoData, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		log.Errorf("ReadRepoFromCar failed for did '%s': %v", did, err)
		return
	}

	_ = repoData.ForEach(ctx, "", func(k string, v cid.Cid) error {
		if !strings.HasPrefix(k, "app.bsky.") {
			return nil
		}

		_, record, err := repoData.GetRecord(ctx, k)
		if err != nil {
			log.Errorf("GetRecord failed for did '%s': %v", k, err)
			return err
		}

		b.processRecord(did, k, record)
		return nil
	})
}

func (b *Backfiller) repoWorker(wg *sync.WaitGroup, repoChan chan *RepoMeta) {
	for repoMeta := range repoChan {
		b.processRepo(repoMeta)
	}
	wg.Done()
}
