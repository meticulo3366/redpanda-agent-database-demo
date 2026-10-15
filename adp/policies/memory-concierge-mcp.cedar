permit(
  principal == Agent::"memory-concierge",
  action in [Action::"McpServer.initialize", Action::"McpServer.ping", Action::"McpServer.tools_list", Action::"McpServerTool.call"],
  resource == McpServer::"crdb-agent-memory"
);
