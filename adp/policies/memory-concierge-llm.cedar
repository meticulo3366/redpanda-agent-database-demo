permit(
  principal == Agent::"memory-concierge",
  action == Action::"LLMProvider.invoke",
  resource
) when {
  resource in [LLMProvider::"zeke-anthropic-agent-memory", LLMProvider::"zeke-bedrock-agent-memory"]
};
