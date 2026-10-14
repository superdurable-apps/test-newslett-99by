//go:build apptest

package apptest_test

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/superdurable/dex-connectors-library/connectors/slack"
	"github.com/superdurable/dex-connectors-library/connectors/superdurable/llm"
	"github.com/superdurable/dex-connectors-library/connectors/superdurable/llm/llmmock"
	"github.com/superdurable/dex-connectors-library/sdkgo"
	"github.com/superdurable/dex-connectors-library/sdkgo/connectormock"
	"github.com/superdurable/dex-template-basic-process/internal/api/generated"
	"github.com/superdurable/dex-template-basic-process/internal/apptest"
	"github.com/superdurable/dex-template-basic-process/internal/process"
)

func TestNewsletterRevisionRequiresLatestApproval(t *testing.T) {
	connections, mock := newMocks(t)
	mock.slack.Respond(slackDraftSent("9999999999.000001"), slackDraftSent("9999999999.000003"))
	revised := connectormock.MustDecodeOutput[llm.GenerateTextResponse](`{"text":"Revised report: clearer release details.","requestedModel":"mock-model","servedModel":"mock-model-2026-01","responseId":"resp-revised","finishReason":"stop","usage":{"inputTokens":30,"outputTokens":7,"totalTokens":37}}`)
	mock.model.GenerateText().When(func(input llm.GenerateTextRequest) bool {
		return len(input.Messages) > 0 && strings.Contains(input.Messages[0].Text, "make it clearer")
	}, llmmock.GenerateTextGenerated(revised, nil))
	env := apptest.Start(t, connections)
	subscribe(t, env, "review-"+uuid.NewString()+"@example.com", uuid.New())
	id := startCampaign(t, env, campaignInput())
	draft := awaitCampaign(t, env, id, generated.NewsletterStatusAwaitingApproval)
	if draft.DraftRevision != 1 || len(mock.slack.Calls()) != 1 || len(mock.gmail.SendMessage().Calls()) != 0 {
		t.Fatalf("initial draft did not wait for approval: %+v", draft)
	}
	config := slack.ThreadReplyCreatedTriggerConfiguration{ChannelID: "C123", ThreadReplyMatcher: slack.MessageMatcher{PosterUserIDs: []string{"UADMIN"}}}
	filter, err := process.NewNewsletterReviewFilter(config)
	if err != nil {
		t.Fatal(err)
	}
	event := sdkgo.TriggerEvent[slack.MessageEvent]{ID: "review-1", Payload: slack.MessageEvent{TeamID: "T123", ChannelID: "C123", ThreadTimestamp: mock.slack.Calls()[0].Input.ThreadTimestamp, Timestamp: "9999999999.000002", UserID: "UADMIN", Text: "revise: make it clearer"}}
	if !filter(event) || process.ResolveNewsletterReviewFlowID(event) != id {
		t.Fatal("valid review did not route to campaign")
	}
	outsider := event
	outsider.Payload.UserID = "UOTHER"
	if filter(outsider) {
		t.Fatal("unapproved user passed trigger filter")
	}
	var outsiderReceipt process.NewsletterReviewReceipt
	err = env.Dex.InvokeRPC(env.Context(t), id, process.NewsletterCampaignFlow{}.ReceiveNewsletterReply, process.MapNewsletterReview(outsider), &outsiderReceipt)
	if err != nil || outsiderReceipt.Accepted {
		t.Fatalf("unapproved user reached campaign review: %+v, %v", outsiderReceipt, err)
	}
	if reviewCampaign(t, env, id, mock, "approve", "9999999999.000000").Accepted {
		t.Fatal("stale approval accepted")
	}
	if reviewCampaign(t, env, id, mock, "revise:", "9999999999.000002").Accepted {
		t.Fatal("empty revision accepted")
	}
	if !reviewCampaign(t, env, id, mock, "revise: make it clearer", "9999999999.000002").Accepted {
		t.Fatal("revision was not accepted")
	}
	env.Eventually(t, 30*time.Second, func() bool {
		next, err := env.API.GetNewsletter(env.Context(t), generated.GetNewsletterParams{CampaignId: id})
		return err == nil && next.Status == generated.NewsletterStatusAwaitingApproval && next.DraftRevision == 2
	})
	second, err := env.API.GetNewsletter(env.Context(t), generated.GetNewsletterParams{CampaignId: id})
	if err != nil || second.DraftTimestamp != "9999999999.000003" || !strings.Contains(second.Report, "Revised report") || len(mock.slack.Calls()) != 2 || len(mock.gmail.SendMessage().Calls()) != 0 {
		t.Fatalf("revision did not produce a new draft: %+v, %v", second, err)
	}
	if reviewCampaign(t, env, id, mock, "approve", "9999999999.000002").Accepted {
		t.Fatal("old approval accepted after revision")
	}
	if !reviewCampaign(t, env, id, mock, "approve", "9999999999.000004").Accepted {
		t.Fatal("latest draft was not approved")
	}
	done := awaitCampaign(t, env, id, generated.NewsletterStatusCompleted)
	if done.DraftRevision != 2 || len(mock.model.GenerateText().Calls()) != 1 || len(mock.gmail.SendMessage().Calls()) != int(done.RecipientCount) {
		t.Fatalf("approved revision was not delivered once: %+v", done)
	}
}

func TestNewsletterDraftPostBranches(t *testing.T) {
	cases := []struct {
		name   string
		result connectormock.Case[slack.PostMessageOutput]
		status generated.NewsletterStatus
	}{
		{"rejected", connectormock.Branch(slack.PostThreadReplyBranchProviderRejected, slack.PostMessageOutput{}, nil), generated.NewsletterStatusDraftFailed},
		{"uncertain", connectormock.Uncertain(slack.PostMessageOutput{}, sdkgo.Failure{Kind: sdkgo.FailureTransport, Message: "connection lost after dispatch"}), generated.NewsletterStatusDraftUnknown},
		{"defect", connectormock.Branch(slack.PostThreadReplyBranchDefect, slack.PostMessageOutput{}, nil), generated.NewsletterStatusDraftFailed},
		{"missingReceipt", connectormock.Branch(slack.PostThreadReplyBranchSent, slack.PostMessageOutput{}, nil), generated.NewsletterStatusDraftUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			connections, mock := newMocks(t)
			mock.slack.Default(tc.result)
			env := apptest.Start(t, connections)
			report := awaitCampaign(t, env, startCampaign(t, env, campaignInput()), tc.status)
			if report.SentCount != 0 || len(mock.gmail.SendMessage().Calls()) != 0 || len(mock.slack.Calls()) != 1 {
				t.Fatalf("failed or unknown Slack draft sent email: %+v", report)
			}
		})
	}
}
