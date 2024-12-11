import {
  ENABLE_CHAT as ENABLE_CHAT_ENV,
  MISTRAL_AGENT_ID_KEY,
  MISTRAL_API_ENDPOINT_KEY,
  MISTRAL_API_KEY,
} from '../../../utils/environment-variables'

const apiKey = import.meta.env[MISTRAL_API_KEY]
const ENABLE_CHAT = import.meta.env[ENABLE_CHAT_ENV]
const MISTRAL_API_ENDPOINT = import.meta.env[MISTRAL_API_ENDPOINT_KEY]
const MISTRAL_AGENT_ID = import.meta.env[MISTRAL_AGENT_ID_KEY]

interface ChatCompletionChunk {
  id: string
  object: string
  created: number
  model: string
  choices: {
    index: number
    delta: {
      content: string
      finish_reason?: string
    }
  }[]
}

export const POST = async (request: Request) => {
  /**
   * Gestion des erreurs de configuration
   */
  if (!apiKey) throw `${MISTRAL_API_KEY} is not set on netlify or is empty`
  if (!MISTRAL_API_ENDPOINT)
    throw `${MISTRAL_API_ENDPOINT_KEY} is not set on netlify or is empty`

  /**
   * Désactivation du service si non configuré
   */
  if (!ENABLE_CHAT)
    return {
      statusCode: 405,
      body: JSON.stringify({
        error: 'There is no chat ATM',
      }),
    }
  const requestBody = await request.json()
  if (!requestBody) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        error: 'Request body is missing',
      }),
    }
  }

  /**
   * Début concret de la fonction
   */

  const body = getReadableStream(
    fetchMistralApi(MISTRAL_API_ENDPOINT, requestBody.message),
  )
  return new Response(body, {
    headers: {
      'Content-Type': 'text/event-stream',
    },
  })
}

/**
 * Méthode qui crée un readable stream à partir d'une promesse
 *
 * @param responsePromise promesse contenant la réponse de mistral
 * @returns un readable stream à retourner dans la lambda
 */
function getReadableStream(responsePromise: Promise<Response>) {
  return new ReadableStream({
    async start(controller) {
      try {
        const response = await responsePromise

        const body = response.body
        if (!body) {
          throw new Error(
            'No response body received from the streaming endpoint.',
          )
        }

        await queueStreamingBody(body, controller)
      } catch (error) {
        console.error('Error in streaming:', error)
        controller.error(error)
      }
    },
  })
}

/**
 * Cette méthode transforme les événements SSE de mistral en une liste de mots
 *
 * @param body readable stream commençant chaque message par `data:`
 * @param controller controller dans lequel retourner les objets parsés de la data
 * @returns retourne un objet streamable
 */
async function queueStreamingBody(
  body: ReadableStream<Uint8Array>,
  controller: ReadableStreamDefaultController,
) {
  const encoder = new TextEncoder()

  const decoder = new TextDecoder()
  let buffer = ''

  for await (const chunk of body) {
    buffer += decoder.decode(chunk, { stream: true })
    let boundaryIndex: number | undefined

    // Process each complete SSE event
    while ((boundaryIndex = buffer.indexOf('\n\n')) !== -1) {
      const rawEvent = buffer.slice(0, boundaryIndex).trim()
      buffer = buffer.slice(boundaryIndex + 2)

      if (rawEvent === 'data: [DONE]') {
        controller.close()
        return
      }

      if (rawEvent.startsWith('data: ')) {
        try {
          const content = mapMistralEventToToken<ChatCompletionChunk>(rawEvent)
          if (content) {
            controller.enqueue(encoder.encode(content))
          }
        } catch (error) {
          console.error('Error parsing SSE event:', rawEvent, error)
        }
      }
    }
  }
  controller.close()
}

function mapMistralEventToToken<T extends ChatCompletionChunk>(
  rawEvent: string,
) {
  const jsonData = JSON.parse(rawEvent.slice(6)) as T
  const content = jsonData.choices.at(0)?.delta.content
  return content
}

async function fetchMistralApi(url: string, message: string) {
  return await fetch(url, {
    method: 'POST', // Adjust the method as necessary
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      stream: true,
      messages: [
        {
          role: 'user',
          content: message,
        },
      ],
      agent_id: MISTRAL_AGENT_ID,
    }),
  })
}
