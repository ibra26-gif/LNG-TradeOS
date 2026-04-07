export const config = {
  api: {
    bodyParser: {
      sizeLimit: '20mb',
    },
  },
};

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured' });
  }

  try {
    const body = req.body;
    body.stream = false;

    const headers = {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
    };

    // Only add MCP beta header when MCP servers are explicitly requested
    if (body.mcp_servers && body.mcp_servers.length > 0) {
      headers['anthropic-beta'] = 'mcp-client-2025-04-04';
    }

    const response = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    });

    // Use text() then parse to get useful debug info on failures
    const text = await response.text();
    try {
      const data = JSON.parse(text);
      return res.status(response.status).json(data);
    } catch (parseErr) {
      return res.status(500).json({
        error: 'Failed to parse Anthropic response',
        status: response.status,
        preview: text.slice(0, 300),
      });
    }

  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
