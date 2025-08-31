import Fastify from 'fastify';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';
import telnyx from 'telnyx'; // Telnyx for outbound calls

dotenv.config();

const { OPENAI_API_KEY, TELNYX_API_KEY, TELNYX_CONNECTION_ID, TELNYX_PHONE_NUMBER } = process.env;

if (!OPENAI_API_KEY || !TELNYX_API_KEY || !TELNYX_CONNECTION_ID || !TELNYX_PHONE_NUMBER) {
    console.error('Missing required environment variables. Please check your .env file.');
    process.exit(1);
}

const fastify = Fastify();
fastify.register(fastifyFormbody);
fastify.register(fastifyWs);

// Initialize Telnyx client
const telnyxClient = telnyx(TELNYX_API_KEY);

const SYSTEM_MESSAGE = `You are an AI assistant. If the user says "send me an email", call the send_email function.`;
const VOICE = 'alloy';
const PORT = process.env.PORT || 6000;

const LOG_EVENT_TYPES = [
    'response.content.done',
    'rate_limits.updated',
    'response.done',
    'input_audio_buffer.committed',
    'input_audio_buffer.speech_stopped',
    'input_audio_buffer.speech_started',
    'session.created'
];

// Outbound call endpoint
fastify.post('/outbound-call', async (request, reply) => {
    try {
        const { to } = request.body;
        if (!to) {
            return reply.status(400).send({ error: 'Phone number (to) is required' });
        }

        const call = await telnyxClient.calls.create({
            connection_id: TELNYX_CONNECTION_ID,
            to: to,
            from: TELNYX_PHONE_NUMBER
        });

        reply.send({ success: true, callId: call.data.id });
    } catch (error) {
        console.error('Outbound call error:', error);
        reply.status(500).send({ error: 'Failed to initiate call' });
    }
});

// Existing inbound call handler
fastify.all('/incoming-call', async (request, reply) => {
    console.log(`Host:${request.headers.host}`);
    const texmllResponse = `<?xml version="1.0" encoding="UTF-8"?>
                          <Response>
                              <Say voice="Polly.Joanna">Please wait while we connect your call to the A. I. voice assistant.</Say>
                              <Pause length="1"/>
                              <Say voice="Polly.Joanna">O.K. you can start talking!</Say>
                              <Connect>
                                  <Stream url="wss://${request.headers.host}/media-stream" bidirectionalMode="rtp" />
                              </Connect>
                          </Response>`;

    reply.type('text/xml').send(texmllResponse);
});

// Function to handle OpenAI function calls
function handleFunctionCall(functionName, arguments) {
    console.log(`[FUNCTION CALL] ${functionName} triggered with args:`, arguments);
    
    if (functionName === 'send_email') {
        // Replace with actual email sending logic (e.g., Nodemailer, SendGrid)
        console.log('[EMAIL] Simulating email send:', arguments);
        return { success: true, message: 'Email sent successfully (simulated)' };
    }

    return { error: `Function ${functionName} not implemented` };
}

fastify.register(async (fastify) => {
    fastify.get('/media-stream', { websocket: true }, (connection, req) => {
        console.log('Client connected');

        const openAiWs = new WebSocket('wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01', {
            headers: {
                Authorization: `Bearer ${OPENAI_API_KEY}`,
                "OpenAI-Beta": "realtime=v1"
            }
        });

        let streamSid = null;

        const sendSessionUpdate = () => {
            const sessionUpdate = {
                type: 'session.update',
                session: {
                    turn_detection: { type: 'server_vad' }, // Barge-in enabled
                    input_audio_format: 'g711_ulaw',
                    output_audio_format: 'g711_ulaw',
                    voice: VOICE,
                    instructions: SYSTEM_MESSAGE,
                    modalities: ["text", "audio"],
                    temperature: 0.8,
                    tools: [{
                        type: 'function',
                        name: 'send_email',
                        description: 'Send an email to the user',
                        parameters: {
                            type: 'object',
                            properties: {
                                email_content: {
                                    type: 'string',
                                    description: 'The content of the email to send'
                                }
                            },
                            required: ['email_content']
                        }
                    }]
                }
            };

            console.log('Sending session update:', JSON.stringify(sessionUpdate));
            openAiWs.send(JSON.stringify(sessionUpdate));
        };

        openAiWs.on('open', () => {
            console.log('Connected to the OpenAI Realtime API');
            setTimeout(sendSessionUpdate, 250);
        });

        openAiWs.on('message', (data) => {
            try {
                const response = JSON.parse(data);

                if (LOG_EVENT_TYPES.includes(response.type)) {
                    console.log(`Received event: ${response.type}`, response);
                }

                if (response.type === 'session.updated') {
                    console.log('Session updated successfully:', response);
                }

                // Handle function calls from OpenAI
                if (response.type === 'conversation.function_call') {
                    const { name, arguments } = response.function_call;
                    const result = handleFunctionCall(name, arguments);
                    
                    // Send the function result back to OpenAI
                    openAiWs.send(JSON.stringify({
                        type: 'conversation.function_call_output',
                        call_id: response.call_id,
                        output: result
                    }));
                }

                if (response.type === 'response.audio.delta' && response.delta) {
                    const audioDelta = {
                        event: 'media',
                        media: { payload: Buffer.from(response.delta, 'base64').toString('base64') }
                    };
                    connection.send(JSON.stringify(audioDelta));
                }
            } catch (error) {
                console.error('Error processing OpenAI message:', error, 'Raw message:', data);
            }
        });

        connection.on('message', (message) => {
            try {
                const data = JSON.parse(message);

                switch (data.event) {
                    case 'media':
                        if (openAiWs.readyState === WebSocket.OPEN) {
                            const audioAppend = {
                                type: 'input_audio_buffer.append',
                                audio: data.media.payload
                            };

                            openAiWs.send(JSON.stringify(audioAppend));
                        }
                        break;
                    case 'start':
                        streamSid = data.stream_id;
                        console.log('Incoming stream has started', streamSid);
                        break;
                    default:
                        console.log('Received non-media event:', data.event);
                        break;
                }
            } catch (error) {
                console.error('Error parsing message:', error, 'Message:', message);
            }
        });

        connection.on('close', () => {
            if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close();
            console.log('Client disconnected.');
        });

        openAiWs.on('close', () => {
            console.log('Disconnected from the OpenAI Realtime API');
        });

        openAiWs.on('error', (error) => {
            console.error('Error in the OpenAI WebSocket:', error);
        });
    });
});

fastify.listen({ port: PORT }, (err) => {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(`Server is listening on port ${PORT}`);
});
