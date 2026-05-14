package mcpserver

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerPrompts adds the tinysql-demo prompt to the MCP server.
func (s *Server) registerPrompts() {
	s.mcpSrv.AddPrompt(&mcp.Prompt{
		Name:        "tinysql-demo",
		Description: "Guides an MCP host through an interactive tinySQL analysis workflow for the given topic.",
		Arguments: []*mcp.PromptArgument{
			{
				Name:        "topic",
				Description: "The domain or subject for the demo schema (e.g. e-commerce, library, IoT sensors)",
				Required:    true,
			},
		},
	}, s.handleTinySQLDemoPrompt)
}

func (s *Server) handleTinySQLDemoPrompt(_ context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	topic := strings.TrimSpace(req.Params.Arguments["topic"])
	if topic == "" {
		topic = "general business"
	}

	text := buildDemoPromptText(topic)

	return &mcp.GetPromptResult{
		Description: fmt.Sprintf("tinySQL interactive demo for topic: %s", topic),
		Messages: []*mcp.PromptMessage{
			{
				Role:    "user",
				Content: &mcp.TextContent{Text: text},
			},
		},
	}, nil
}

func buildDemoPromptText(topic string) string {
	return fmt.Sprintf(`You are working with a tinySQL database exposed through the Model Context Protocol (MCP).
tinySQL is a lightweight, embeddable SQL engine written in Go. It supports SELECT, INSERT, UPDATE, DELETE, CREATE TABLE,
JOINs, GROUP BY, HAVING, ORDER BY, LIMIT, CTEs, window functions, views, triggers, full-text search, vector search,
multi-tenancy, and rich sys.* / catalog.* introspection.

Your task is to run a short, interactive data analysis demonstration on the topic: **%s**

Follow these steps in order:

1. **Design a schema** relevant to "%s".
   Design 2–4 tables that model a realistic scenario (e.g. entities, relationships, facts).
   Keep column types to INT, TEXT, FLOAT, BOOL, or DATETIME.

2. **Create the tables** using the `+"`create_table`"+` MCP tool.
   Use a separate `+"`create_table`"+` call for each table.

3. **Insert sample data** using the `+"`write_query`"+` MCP tool.
   Insert at least 5 rows per table that represent realistic data for "%s".

4. **Run analytical queries** using the `+"`read_query`"+` MCP tool.
   Execute at least two or three meaningful queries such as aggregations, JOINs, or filters.
   Explain what each query reveals about the data.

5. **Record key observations** using the `+"`append_insight`"+` MCP tool.
   After each meaningful analytical finding, store a concise insight.
   Record at least two insights.

6. **Read the insight memo** by reading the resource `+"`memo://insights`"+`.
   Summarise what has been learned.

7. **Inspect the schema** by reading `+"`tinysql://schema`"+` or `+"`tinysql://agent-context`"+`.
   Confirm the database structure matches your design.

Important notes:
- This database is backed by tinySQL through MCP. Use only the provided tools.
- Do not attempt to read files or access external resources.
- Keep queries focused and well-explained.
- If a tool returns an error, diagnose it and correct the query before continuing.

Begin the demo now.`, topic, topic, topic)
}
