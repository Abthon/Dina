export default function Home() {
  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', lineHeight: '1.5', padding: '2rem' }}>
      <h1>Twilio WebSocket Server</h1>
      <p>The server is running successfully.</p>
      <p>WebSocket Endpoint: <code>/api/twilio/ws</code></p>
    </div>
  );
}
