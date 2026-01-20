"""MCP Server for Amazon Seller Analytics.

This module provides an MCP (Model Context Protocol) server that exposes
the analytics tools for use with Claude Code and other MCP clients.

Usage:
    # Run the MCP server directly
    python -m app.claude.mcp_server

    # Or use with Claude Code by adding to .claude/config.json:
    {
        "mcpServers": {
            "amazon-analytics": {
                "command": "python",
                "args": ["-m", "app.claude.mcp_server"],
                "cwd": "/path/to/mb_onboarding"
            }
        }
    }
"""

import asyncio
import json
import sys
from typing import Any, Dict, List, Optional

# Check for MCP package
try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import (
        Tool,
        TextContent,
        CallToolResult,
        ListToolsResult,
    )
    MCP_AVAILABLE = True
except ImportError:
    MCP_AVAILABLE = False
    print("MCP package not installed. Install with: pip install mcp", file=sys.stderr)

from .tools import CLAUDE_TOOLS, SYSTEM_PROMPT
from .executor import execute_tool


def create_mcp_server() -> "Server":
    """Create and configure the MCP server."""
    if not MCP_AVAILABLE:
        raise ImportError("MCP package is required. Install with: pip install mcp")

    server = Server("amazon-analytics")

    @server.list_tools()
    async def list_tools() -> List[Tool]:
        """List all available tools."""
        tools = []
        for tool_def in CLAUDE_TOOLS:
            tools.append(Tool(
                name=tool_def["name"],
                description=tool_def["description"],
                inputSchema=tool_def["input_schema"]
            ))
        return tools

    @server.call_tool()
    async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
        """Execute a tool call."""
        # Run the synchronous executor in a thread pool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, execute_tool, name, arguments)

        # Format result as text content
        return [TextContent(
            type="text",
            text=json.dumps(result, indent=2, default=str)
        )]

    return server


async def main():
    """Run the MCP server."""
    if not MCP_AVAILABLE:
        print("Error: MCP package not installed", file=sys.stderr)
        print("Install with: pip install mcp", file=sys.stderr)
        sys.exit(1)

    server = create_mcp_server()

    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())
