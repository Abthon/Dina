/**
 * Custom Next.js Server with Twilio WebSocket Support
 * 
 * This replaces the default Next.js server and adds WebSocket support
 * for Twilio Media Streams while keeping all your existing API routes.
 * 
 * Run with: node server.js
 */

//const { createServer } = require('http');
//require('dotenv').config();
//const { parse } = require('url');
//const next = require('next');
//const WebSocket = require('ws');

//const dev = process.env.NODE_ENV !== 'production';
//const hostname = 'localhost';
//const port = parseInt(process.env.PORT || '3000', 10);

//const app = next({ dev, hostname, port });
//const handle = app.getRequestHandler();

//changed for railway deployment
const { createServer } = require('http');
require('dotenv').config();
const { parse } = require('url');
const next = require('next');
const WebSocket = require('ws');

const dev = process.env.NODE_ENV !== 'production';
const port = process.env.PORT; // 🚨 Railway controls this

const app = next({ dev }); // 🚨 DO NOT pass hostname/port
const handle = app.getRequestHandler();

// end of railway the railway code 

const ADDIS_REALTIME_API_KEY = process.env.ADDIS_REALTIME_API_KEY;
const ADDIS_REALTIME_WS_URL = `wss://relay.addisassistant.com/ws?kb=emm&apiKey=${ADDIS_REALTIME_API_KEY}`

app.prepare().then(() => {
  const server = createServer(async (req, res) => {
    try {
      const parsedUrl = parse(req.url, true);
      await handle(req, res, parsedUrl);
    } catch (err) {
      console.error('Error occurred handling', req.url, err);
      res.statusCode = 500;
      res.end('internal server error');
    }
  });

  // Create WebSocket server on the same HTTP server
  const wss = new WebSocket.Server({ 
    server,
    path: '/api/twilio/ws' // This is the WebSocket endpoint path
  });

  console.log('🚀 Setting up Twilio WebSocket handler on /api/twilio/ws');

  wss.on('connection', (twilioWs, req) => {
    console.log('📞 New Twilio WebSocket connection');
    
    let addisWs = null;
    let callSid = '';
    let streamSid = '';
    let audioBuffer = []; // Buffer audio until Addis AI is ready
    let isAddisReady = false;

    twilioWs.on('message', (message) => {
      try {
        const data = JSON.parse(message);
        switch (data.event) {
          case 'start':
            callSid = data.start.callSid;
            streamSid = data.start.streamSid;
            console.log('📞 Stream started:', { callSid, streamSid });
            connectToAddisAI();
            break;
            
          case 'media':
            handleMedia(data);
            break;
            
          case 'stop':
            console.log('📞 Stream stopped');
            cleanup();
            break;
        }
      } catch (error) {
        console.error('❌ Error processing Twilio message:', error);
      }
    });

    twilioWs.on('close', () => {
      console.log('📞 Twilio connection closed');
      cleanup();
    });

    twilioWs.on('error', (error) => {
      console.error('❌ Twilio WebSocket error:', error);
    });

    function connectToAddisAI() {
      addisWs = new WebSocket(ADDIS_REALTIME_WS_URL);

      addisWs.on('open', () => {
        console.log('✅ Connected to Addis AI relay');
        
        // Send session start/config message matching AddisChatBot.tsx
        const sessionStartMsg = {
          type: "session.start",
          mode: "realtime-audio",
          language: "am",
        };
        addisWs.send(JSON.stringify(sessionStartMsg));
        console.log('Sent session.start message');
        
        isAddisReady = true;
        
        // Flush buffered audio with throttling
        if (audioBuffer.length > 0) {
          console.log(`📤 Flushing ${audioBuffer.length} buffered audio chunks to Addis AI`);
          
          // Send buffered chunks with small delay to avoid overwhelming
          let index = 0;
          const flushInterval = setInterval(() => {
            if (index < audioBuffer.length) {
              sendAudioToAddisAI(audioBuffer[index]);
              index++;
            } else {
              clearInterval(flushInterval);
              audioBuffer = [];
              console.log('✅ Finished flushing buffered audio');
            }
          }, 10); // 10ms between chunks
        }
      });

      addisWs.on('message', (message) => {
        try {
          const data = JSON.parse(message);
          
          // Log ALL messages for debugging
          console.log('📡 RAW Addis AI message:', JSON.stringify(data).substring(0, 500));
          
          if (data.type === 'status') {
            console.log('📡 Addis AI status:', data.message);
          } else if (data.type === 'warning' && data.message) {
            console.log('⚠️ Addis AI warning:', data.message);
          } else if (data.serverContent?.turnComplete) {
            console.log('✅ Turn complete. Duration:', data.usageMetadata?.totalBilledAudioDurationSeconds + 's');
          } else if (data.serverContent?.modelTurn?.parts?.[0]?.inlineData) {
            const audioData = data.serverContent.modelTurn.parts[0].inlineData.data;
            console.log('🔊 Received audio from Addis AI, sending to Twilio...');
            sendAudioToTwilio(audioData);
          } else if (data.serverContent?.interrupted) {
            console.log('🛑 Playback interrupted - clearing Twilio buffer');
            sendClearToTwilio();
          } else if (data.error) {
            console.error('❌ Addis AI error:', JSON.stringify(data.error));
          }
        } catch (error) {
          console.error('❌ Error processing Addis AI message:', error);
          console.error('Raw message:', message.toString().substring(0, 500));
        }
      });

      addisWs.on('close', () => {
        console.log('📡 Addis AI connection closed');
      });

      addisWs.on('error', (error) => {
        console.error('❌ Addis AI error:', error);
      });
    }

    let audioChunksSent = 0;
    
    // Buffer for outgoing audio (Int16 bytes)
    let outgoingAudioBuffer = Buffer.alloc(0);
    const TARGET_CHUNK_BYTES = 4096; // 2048 Int16 samples * 2 bytes

    function sendAudioToAddisAI(base64AudioData) {
      if (!addisWs || addisWs.readyState !== WebSocket.OPEN) {
        return;
      }
      
      // Use simpler data format matching AddisChatBot.tsx
      const message = {
        data: base64AudioData
      };
      
      addisWs.send(JSON.stringify(message));
      audioChunksSent++;
      
      if (audioChunksSent % 50 === 0) {
        console.log(`🎤 Sent ${audioChunksSent} total audio chunks to Addis AI`);
      }
    }

    function handleMedia(data) {
      const payload = data.media.payload;
      const mulawBuffer = Buffer.from(payload, 'base64');
      const pcmBuffer = mulawToPcm(mulawBuffer);
      const resampled16k = resample8kTo16k(pcmBuffer);
      
      // Convert Int16Array to Buffer (raw bytes)
      const currentChunk = Buffer.from(resampled16k.buffer);
      
      // Append to outgoing buffer
      outgoingAudioBuffer = Buffer.concat([outgoingAudioBuffer, currentChunk]);
      
      // Only send if we have enough data (matching AddisChatBot.tsx chunk size)
      if (outgoingAudioBuffer.length >= TARGET_CHUNK_BYTES) {
        // Extract chunk
        const chunkToSend = outgoingAudioBuffer.slice(0, TARGET_CHUNK_BYTES);
        outgoingAudioBuffer = outgoingAudioBuffer.slice(TARGET_CHUNK_BYTES);
        
        // Convert to base64
        const base64Audio = chunkToSend.toString('base64');
        
        if (!isAddisReady) {
          audioBuffer.push(base64Audio);
          if (audioBuffer.length === 1) console.log('📦 Buffering audio...');
          return;
        }

        sendAudioToAddisAI(base64Audio);
        
        // Log RMS occasionally
        if (audioChunksSent % 50 === 0) {
           const int16Data = new Int16Array(chunkToSend.buffer, chunkToSend.byteOffset, chunkToSend.length / 2);
           let sum = 0;
           for (let i = 0; i < int16Data.length; i++) {
             sum += int16Data[i] * int16Data[i];
           }
           const rms = Math.sqrt(sum / int16Data.length);
           console.log(`🎤 Sending chunk (RMS: ${Math.round(rms)})`);
        }
      }
    }

    function sendClearToTwilio() {
      if (!twilioWs || twilioWs.readyState !== WebSocket.OPEN) {
        return;
      }
      
      try {
        const message = {
          event: 'clear',
          streamSid: streamSid,
        };
        
        twilioWs.send(JSON.stringify(message));
      } catch (error) {
        console.error('❌ Error sending clear to Twilio:', error);
      }
    }

    function sendAudioToTwilio(base64AudioData) {
      if (!twilioWs || twilioWs.readyState !== WebSocket.OPEN) {
        console.log('⚠️ Twilio WebSocket not ready');
        return;
      }

      try {
        const pcmBuffer = Buffer.from(base64AudioData, 'base64');
        const int16Array = new Int16Array(
          pcmBuffer.buffer,
          pcmBuffer.byteOffset,
          pcmBuffer.length / 2
        );
        
        const resampled8k = resample24kTo8k(int16Array);
        const mulawBuffer = pcmToMulaw(resampled8k);
        
        const chunkSize = 160; // 20ms at 8kHz
        let chunksCount = 0;
        for (let i = 0; i < mulawBuffer.length; i += chunkSize) {
          const chunk = mulawBuffer.slice(i, Math.min(i + chunkSize, mulawBuffer.length));
          const payload = chunk.toString('base64');
          
          twilioWs.send(JSON.stringify({
            event: 'media',
            streamSid: streamSid,
            media: { payload },
          }));
          chunksCount++;
        }
        console.log(`📞 Sent ${chunksCount} audio chunks to Twilio`);
      } catch (error) {
        console.error('❌ Error sending audio to Twilio:', error);
      }
    }

    function cleanup() {
      if (addisWs) {
        addisWs.close();
        addisWs = null;
      }
    }

    // Audio conversion utilities
    function mulawToPcm(mulawBuffer) {
      const pcm = new Int16Array(mulawBuffer.length);
      const MULAW_BIAS = 0x84;

      for (let i = 0; i < mulawBuffer.length; i++) {
        let mulaw = ~mulawBuffer[i];
        const sign = mulaw & 0x80;
        const exponent = (mulaw >> 4) & 0x07;
        const mantissa = mulaw & 0x0F;
        
        let sample = ((mantissa << 3) + MULAW_BIAS) << exponent;
        sample = sign ? -sample : sample;
        pcm[i] = sample;
      }
      
      return pcm;
    }

    function pcmToMulaw(pcmBuffer) {
      const mulaw = Buffer.alloc(pcmBuffer.length);
      const MULAW_MAX = 0x1FFF;
      const MULAW_BIAS = 0x84;

      for (let i = 0; i < pcmBuffer.length; i++) {
        let sample = pcmBuffer[i];
        const sign = sample < 0 ? 0x80 : 0x00;
        if (sign) sample = -sample;
        
        sample = Math.min(sample, MULAW_MAX);
        sample += MULAW_BIAS;
        
        let exponent = 7;
        for (let exp = 0; exp < 8; exp++) {
          if (sample <= (0xFF << exp)) {
            exponent = exp;
            break;
          }
        }
        
        const mantissa = (sample >> (exponent + 3)) & 0x0F;
        mulaw[i] = ~(sign | (exponent << 4) | mantissa);
      }
      
      return mulaw;
    }

    function resample8kTo16k(pcmBuffer) {
      const output = new Int16Array(pcmBuffer.length * 2);
      for (let i = 0; i < pcmBuffer.length - 1; i++) {
        output[i * 2] = pcmBuffer[i];
        output[i * 2 + 1] = Math.floor((pcmBuffer[i] + pcmBuffer[i + 1]) / 2);
      }
      output[output.length - 2] = pcmBuffer[pcmBuffer.length - 1];
      output[output.length - 1] = pcmBuffer[pcmBuffer.length - 1];
      return output;
    }

    function resample24kTo8k(pcmBuffer) {
      const output = new Int16Array(Math.floor(pcmBuffer.length / 3));
      for (let i = 0; i < output.length; i++) {
        output[i] = pcmBuffer[i * 3];
      }
      return output;
    }
  });

  server.once('error', (err) => {
    console.error(err);
    process.exit(1);
  });

  // Commented out original code for railway deployment  
  //server.listen(port, () => {
  //  console.log(`✅ Server ready on http://${hostname}:${port}`);
  //  console.log(`📞 Twilio Voice Webhook: http://${hostname}:${port}/api/twilio/voice`);
  //  console.log(`📡 Twilio WebSocket: ws://${hostname}:${port}/api/twilio/ws`);
  //});
  // End of the original code 

  server.listen(port, '0.0.0.0', () => {
    console.log(`✅ Server ready on port ${port}`);
    console.log(`📞 Twilio Voice Webhook: https://wss-dina.up.railway.app/api/twilio/voice`);
    console.log(`📡 Twilio WebSocket: wss://wss-dina.up.railway.app/api/twilio/ws`);
  });
});
