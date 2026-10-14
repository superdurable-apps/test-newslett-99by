package worker

import (
	"github.com/superdurable/dex-template-basic-process/internal/process"
	"github.com/superdurable/dex/sdk-go/dex"
)

func NewsletterFlow(connections Connections, clients *process.Clients) process.NewsletterCampaignFlow {
	return process.NewsletterCampaignFlow{
		GitHub: connections.GitHub, Slack: connections.Slack, LLM: connections.OpenAI,
		Gmail: connections.Gmail, AllowedAdmins: clients.AllowedAdmins, Clients: clients,
	}
}

// Application is the single Flow list shared by the Worker and API tests.
func Application(connections Connections, clients *process.Clients) []dex.Flow {
	return []dex.Flow{
		process.SubscriptionRegistryFlow{},
		NewsletterFlow(connections, clients),
	}
}
