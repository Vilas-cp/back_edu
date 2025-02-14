import { NextResponse } from 'next/server';
import { GoogleGenerativeAI } from '@google/generative-ai';
import { RateLimiterMemory } from 'rate-limiter-flexible';




const rateLimiters = {
  perMinute: new RateLimiterMemory({
    points: 15, 
    duration: 60, 
  }),
  perHour: new RateLimiterMemory({
    points: 250,
    duration: 60 * 60, 
  }),
  perDay: new RateLimiterMemory({
    points: 500, 
    duration: 24 * 60 * 60, 
  }),
};

async function checkRateLimit(ip: string) {
  try {
    await rateLimiters.perMinute.consume(ip);
    await rateLimiters.perHour.consume(ip);
    await rateLimiters.perDay.consume(ip);
  } catch (error: unknown) {
    if (error instanceof Error) {
      throw new Error('Rate limit exceeded');
    } else if (typeof error === 'object' && error !== null && 'msBeforeNext' in error) {
      const retryAfter = Math.ceil((error as { msBeforeNext: number }).msBeforeNext / 1000);
      throw new Error(`Rate limit exceeded. Try again in ${retryAfter} seconds.`);
    } else {
      throw new Error('An unknown error occurred while checking rate limits.');
    }
  }
}
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY || '');

export async function POST(request: Request) {
  try {
    // Extract client IP address
    const ip = request.headers.get('x-forwarded-for') || request.headers.get('x-real-ip') || 'unknown-ip';

    // Check rate limits
    await checkRateLimit(ip);

    const { messages, stream } = await request.json();

    if (!messages || !Array.isArray(messages)) {
      return NextResponse.json({ error: 'Invalid request body' }, { status: 400 });
    }

    const model = genAI.getGenerativeModel({ model: 'gemini-pro' });

    const prompt = messages
      .map((msg: { role: string; content: string }) => `${msg.role}: ${msg.content}`)
      .join('\n');

    const result = await model.generateContent(prompt);
    const response = await result.response;
    const content = response.text();
    const chunkSize = 20;

    if (stream) {
      const encoder = new TextEncoder();

      const streamResponse = new ReadableStream({
        async start(controller) {
          const words = content.split(' ');
          let buffer = '';

          for (const word of words) {
            buffer += `${word} `;

            if (buffer.endsWith('.') || buffer.endsWith('!') || buffer.endsWith('?') || buffer.length >= chunkSize) {
              controller.enqueue(encoder.encode(`data: ${JSON.stringify({ content: buffer.trim() })}\n\n`));
              buffer = '';
            }

            await new Promise((resolve) => setTimeout(resolve, 100));
          }

          if (buffer) {
            controller.enqueue(encoder.encode(`data: ${JSON.stringify({ content: buffer.trim() })}\n\n`));
          }

          controller.close();
        },
      });

      return new Response(streamResponse, {
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          Connection: 'keep-alive',
        },
      });
    } else {
      return NextResponse.json({ content });
    }
  } catch (error: unknown) {
    console.error('Error:', error);

    if (error instanceof Error && error.message.startsWith('Rate limit exceeded')) {
      return NextResponse.json({ error: error.message }, { status: 429 });
    }

    return NextResponse.json({ error: 'Failed to generate content' }, { status: 500 });
  }
}
