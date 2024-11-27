package backfill

import (
	"bsky/storage"
	"bsky/storage/models"
	"bsky/utils"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	comatproto "github.com/bluesky-social/indigo/api/atproto"
	appbsky "github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/repo"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/ipfs/go-cid"
	log "github.com/sirupsen/logrus"
	"github.com/whyrusleeping/cbor-gen"
	"go.uber.org/zap"
	"hash"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	AccountDeactivatedError = "AccountDeactivated"
	InvalidRequestError     = "InvalidRequest" // Seen when profile is not found
	ExpiredToken            = "ExpiredToken"
)
const NumRepoWorkers = 128
const NumMetadataWorkers = 32

type Backfiller struct {
	serviceName      string
	storageManager   *storage.Manager
	client           *xrpc.Client
	languageDetector *utils.LanguageDetector
	hasher           hash.Hash32
}

func getXrpcClient() *xrpc.Client {
	ctx := context.Background()

	xrpcClient := &xrpc.Client{
		Host:   "https://bsky.social",
		Client: http.DefaultClient,
	}

	auth, err := comatproto.ServerCreateSession(ctx, xrpcClient, &comatproto.ServerCreateSession_Input{
		Identifier: os.Getenv("STATISTICS_USER"),
		Password:   os.Getenv("STATISTICS_PASSWORD"),
	})
	if err != nil {
		zap.L().Error("create session failed", zap.Error(err), zap.String("username", os.Getenv("STATISTICS_USER")))

		return nil
	}

	xrpcClient.Auth = &xrpc.AuthInfo{
		AccessJwt:  auth.AccessJwt,
		RefreshJwt: auth.RefreshJwt,
		Handle:     auth.Handle,
		Did:        auth.Did,
	}

	return xrpcClient
}

func NewBackfiller(serviceName string, storageManager *storage.Manager) *Backfiller {
	return &Backfiller{
		serviceName:      serviceName,
		storageManager:   storageManager,
		client:           getXrpcClient(),
		languageDetector: utils.NewLanguageDetector(),
		hasher:           fnv.New32a(),
	}
}

func (b *Backfiller) Run() {
	ctx := context.Background()

	// Span workers
	wg := sync.WaitGroup{}
	repoChan := make(chan *comatproto.SyncListRepos_Repo)
	metadataChan := make(chan string, 1000000)

	for i := 0; i < NumRepoWorkers; i++ {
		wg.Add(1)
		go b.repoWorker(&wg, repoChan, metadataChan)
	}
	for i := 0; i < NumMetadataWorkers; i++ {
		wg.Add(1)
		go b.metadataWorker(&wg, metadataChan)
	}

	cursor := b.storageManager.GetCursor(b.serviceName)
	for {
		response, err := comatproto.SyncListRepos(ctx, b.client, cursor, 1000)
		if err != nil {
			log.Errorf("SyncListRepos failed: %v", err)
			time.Sleep(30 * time.Second)
			continue
		}

		for _, repoDescription := range response.Repos {
			if repoDescription.Active == nil || !*repoDescription.Active {
				continue
			}
			repoChan <- repoDescription
		}

		if response.Cursor == nil {
			break
		}
		cursor = *response.Cursor
		b.storageManager.UpdateCursor(b.serviceName, cursor)
	}

	close(repoChan)
	close(metadataChan)
	wg.Wait()
}

func (b *Backfiller) getPlcProfile(did string) (Profile, error) {
	var profile Profile
	response, err := http.Get(
		fmt.Sprintf("https://plc.directory/%s/log/audit", did),
	)
	if err != nil {
		return profile, err
	}
	defer response.Body.Close()

	responseBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return profile, err
	}

	var plcProfileData []PlcLogAuditEntry
	err = json.Unmarshal(responseBytes, &plcProfileData)
	if err != nil {
		return profile, err
	}

	if len(plcProfileData) == 0 || len(plcProfileData[len(plcProfileData)-1].Operation.AlsoKnownAs) == 0 {
		return profile, errors.New("missing plc data")
	}

	alsoKnownAs := plcProfileData[len(plcProfileData)-1].Operation.AlsoKnownAs[0]
	profile.Handle = strings.Replace(alsoKnownAs, "at://", "", -1)

	profile.CreatedAt = plcProfileData[0].CreatedAt

	return profile, nil
}

func (b *Backfiller) handleFollowCreate(did string, uri string, follow *appbsky.GraphFollow) {
	uriParts := strings.Split(uri, "/")
	uriKey := uriParts[len(uriParts)-1]

	createdAt, err := utils.ParseTime(follow.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return
	}

	go func() {
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
	}()
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

	createdAt, err := utils.ParseTime(createdAtStr)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return
	}

	go func() {
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
	}()
}

func (b *Backfiller) handlePostCreate(did string, uri string, post *appbsky.FeedPost) {
	createdAt, err := utils.ParseTime(post.CreatedAt)
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

	go func() {
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

	}()
}

func (b *Backfiller) metadataWorker(wg *sync.WaitGroup, metadataChan chan string) {
	for did := range metadataChan {
		b.setCreatedAt(did, false)
	}
	wg.Done()
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
				case ExpiredToken:
					b.client = getXrpcClient()
					retry = true
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

func (b *Backfiller) processRepo(repoMeta *comatproto.SyncListRepos_Repo) {
	ctx := context.Background()
	log.Warnf("Backfilling '%s'", repoMeta.Did)

	repoBytes, err := comatproto.SyncGetRepo(ctx, b.client, repoMeta.Did, "")
	if err != nil {
		log.Errorf("SyncGetRepo failed for did '%s': %v", repoMeta.Did, err)
		b.processBskyError(repoMeta.Did, err, func() { b.processRepo(repoMeta) })
		return
	}

	repoData, err := repo.ReadRepoFromCar(ctx, bytes.NewReader(repoBytes))
	if err != nil {
		log.Errorf("ReadRepoFromCar failed for did '%s': %v", repoMeta.Did, err)
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

		b.processRecord(repoMeta.Did, k, record)
		return nil
	})
}

func (b *Backfiller) repoWorker(
	wg *sync.WaitGroup,
	repoChan chan *comatproto.SyncListRepos_Repo,
	metadataChan chan string,
) {
	for repoMeta := range repoChan {
		b.processRepo(repoMeta)
		metadataChan <- repoMeta.Did
	}
	wg.Done()
}

func (b *Backfiller) setCreatedAt(did string, askBluesky bool) {
	var profile Profile

	if strings.HasPrefix(did, "did:plc") && !askBluesky {
		plcProfile, err := b.getPlcProfile(did)
		if err != nil {
			log.Errorf("Error getting plc profile: %v", err)
			b.setCreatedAt(did, true)
			return
		}
		profile = plcProfile
	} else {
		bskyProfile, err := appbsky.ActorGetProfile(context.Background(), b.client, did)
		if err != nil {
			log.Errorf("Error getting actor '%s': %v", did, err)
			b.processBskyError(did, err, func() { b.setCreatedAt(did, true) })
			return
		}
		if bskyProfile.CreatedAt == nil {
			return
		}
		profile = Profile{
			Handle:    bskyProfile.Handle,
			CreatedAt: *bskyProfile.CreatedAt,
		}
	}

	createdAt, err := utils.ParseTime(profile.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return
	}

	b.storageManager.SetUserMetadata(did, profile.Handle, createdAt)
}
