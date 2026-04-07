export default function handler(req, res) {
  res.status(200).json({
    ok: true,
    method: req.method,
    hasApiKey: !!process.env.ANTHROPIC_API_KEY,
    bodyType: typeof req.body,
    bodyKeys: req.body ? Object.keys(req.body) : [],
    timestamp: new Date().toISOString(),
  });
}
 
