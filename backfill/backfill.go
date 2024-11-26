package backfill

import (
	"bsky/storage"
	"bsky/storage/models"
	"bsky/utils"
	"bytes"
	"context"
	"errors"
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
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	AccountDeactivatedError = "AccountDeactivated"
	InvalidRequestError     = "InvalidRequest" // Seen when profile is not found
	ExpiredToken            = "ExpiredToken"
)
const NumWorkers = 128

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
	c := make(chan *comatproto.SyncListRepos_Repo)
	for i := 0; i < NumWorkers; i++ {
		go b.worker(c)
	}

	cursor := b.storageManager.GetCursor(b.serviceName)
	for {
		response, err := comatproto.SyncListRepos(ctx, b.client, cursor, 1000)
		if err != nil {
			log.Errorf("SyncListRepos failed: %v", err)
			time.Sleep(10 * time.Second)
			continue
		}

		for _, repoMeta := range response.Repos {
			if repoMeta.Active == nil || !*repoMeta.Active {
				continue
			}
			c <- repoMeta
		}

		if response.Cursor == nil {
			break
		}
		cursor = *response.Cursor
		b.storageManager.UpdateCursor(b.serviceName, cursor)
	}

	close(c)
}

func (b *Backfiller) handleFollowCreate(did string, uri string, follow *appbsky.GraphFollow) {
	uriParts := strings.Split(uri, "/")
	uriKey := uriParts[len(uriParts)-1]

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
			},
		)
	}()
}

func (b *Backfiller) handleInteractionCreate(did string, uri string, kind models.InteractionType, createdAtStr string, subjectUri string) {
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
		b.storageManager.CreateInteraction(
			models.Interaction{
				UriKey:       uriKey,
				Kind:         kind,
				AuthorID:     authorId,
				PostUriKey:   postUriKey,
				PostAuthorId: postAuthorId,
				CreatedAt:    createdAt,
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

	var replyParent []string
	var replyRoot []string

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
			replyParent = []string{strconv.Itoa(int(authorId)), uriKey}
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
			replyRoot = []string{strconv.Itoa(int(authorId)), uriKey}
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
				UriKey:      uriKey,
				AuthorId:    authorId,
				AuthorDid:   did,
				ReplyParent: replyParent,
				ReplyRoot:   replyRoot,
				CreatedAt:   createdAt,
				Language:    language,
				Rank:        rank,
				Text:        post.Text,
				Embed:       post.Embed,
			})

	}()
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

func (b *Backfiller) setCreatedAt(did string) {
	profile, err := appbsky.ActorGetProfile(context.Background(), b.client, did)
	if err != nil {
		log.Errorf("Error getting actor '%s': %v", did, err)
		b.processBskyError(did, err, func() { b.setCreatedAt(did) })
		return
	}
	if profile.CreatedAt == nil {
		return
	}

	createdAt, err := utils.ParseTime(*profile.CreatedAt)
	if err != nil {
		log.Errorf("Error parsing created at: %s", err)
		return
	}

	b.storageManager.SetUserMetadata(did, profile.Handle, createdAt)
}

func (b *Backfiller) worker(c chan *comatproto.SyncListRepos_Repo) {
	for repoMeta := range c {
		b.processRepo(repoMeta)
		b.setCreatedAt(repoMeta.Did)
	}
}
