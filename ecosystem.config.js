module.exports = {
  apps: [
    {
      name: 'reqable-mcp',
      script: './go/bin/reqable-mcp.exe',
      cwd: __dirname,
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      env: {
        REQABLE_INGEST_HOST: '0.0.0.0',
        REQABLE_MCP_HOST: '0.0.0.0',
      },
    },
  ],
};
