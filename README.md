# Super Durable Newsletter

Visitors subscribe through the application's web page. Each normalized email address is retained once in `SubscriptionRegistryFlow`.

An authorized administrator publishes a newsletter by posting exactly `publish newsletter` as a new top-level message in the configured Slack channel. The Slack message identity starts one `NewsletterCampaignFlow`; duplicate delivery of the same message does not start another campaign. The campaign freezes its subscriber list and seven-day window at the start, reads every available release page from `superdurable/dex` and `superdurable/dex-connectors-library`, excludes drafts and releases outside that window, and asks the configured OpenAI model to summarize the releases with links. If there are no releases, it prepares a clear no-releases notice. It posts the report as a draft reply in the command's Slack thread, then waits. An authorized administrator replies exactly `approve` to send it, or starts a reply with `revise:` and feedback to request an LLM revision. Each revision appears as a new draft reply and requires a new approval. Once approved, it sends one Gmail message per subscriber and records sent, failed, or uncertain delivery outcomes. An uncertain Gmail response is not retried automatically.

## Studio configuration

Configure these four connections in Studio before running the application:

- `newsletter-slack`: authorize the Slack app and configure both `publish-newsletter` and `review-newsletter` trigger bindings for the same joined channel and the same administrator members. The app needs permission to post thread replies. Publish with a top-level `publish newsletter` message; review by replying in that message's thread. If Studio cannot load picker choices, enter the exact Slack channel and member IDs.
- `newsletter-github`: authorize GitHub access for release listing.
- `newsletter-openai`: configure the OpenAI provider for report generation.
- `newsletter-gmail`: authorize the Google account that sends newsletter email.

Studio provides Flow runs and their display. The application also exposes `GET /api/newsletters/{campaignId}` for a campaign's report and aggregate delivery status. The campaign ID derives from the Slack team, channel and message timestamp. The subscriber page uses `POST /api/subscriptions` and requires no login.

## Source verification

`make check-fdg-v2` validates the Flow definitions. `make test-app RUN='TestSubscriptionValidationAndDeduplication|TestNewsletter'` runs the HTTP API tests against the test Dex engine with connector mocks. These tests verify application source and do not establish that the configured Slack, GitHub, OpenAI, or Gmail accounts work. Verify those integrations in configured Preview before Live use.
