import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda'
import 'source-map-support/register'
import * as middy from 'middy'
import { cors } from 'middy/middlewares'
import { CreateTodoRequest } from '../../requests/CreateTodoRequest'
import { createTodo } from '../../dataLayer/todosAccess'
import { todoBuilder } from '../../businessLogic/todos'

export const handler = middy(
  async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
    const newTodo: CreateTodoRequest = JSON.parse(event.body)
    // TODO: Implement creating a new TODO item
    const todo = todoBuilder(newTodo, event)
    const createdTodo = await createTodo(todo)

    return {
      statusCode: 201,
      body: JSON.stringify({
        item: createdTodo
      })
    }
  }
)

handler.use(
  cors({
    credentials: true
  })
)
