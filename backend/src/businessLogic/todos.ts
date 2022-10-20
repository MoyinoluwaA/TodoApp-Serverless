import { APIGatewayProxyEvent } from 'aws-lambda';
import { TodoItem } from '../models/TodoItem'
import { CreateTodoRequest } from '../requests/CreateTodoRequest'
import { UpdateTodoRequest } from '../requests/UpdateTodoRequest';
import { createLogger } from '../utils/logger'
import * as uuid from 'uuid'
import { getUserId } from '../lambda/utils';
import { TodosAccess } from '../dataLayer/todosAccess'
import { getUploadUrl } from '../dataLayer/attachmentUtils';

const todoAccess = new TodosAccess()
const bucketName = process.env.ATTACHMENT_S3_BUCKET;

// // TODO: Implement businessLogic
export async function createTodo(todoRequest: CreateTodoRequest, event: APIGatewayProxyEvent): Promise<TodoItem> {
    const todoId = uuid.v4()
    const todo = {
      todoId,
      userId: getUserId(event),
      createdAt: new Date().toISOString(),
      done: false,
      attachmentUrl: '',
      ...todoRequest
    }

    const logger = createLogger('CreateTodo')
    logger.info('New Todo', todo)
    
    return await todoAccess.createTodo(todo)
}

export async function getAllTodos(userId: string): Promise<TodoItem[]> {
  return await todoAccess.getAllTodosByUserId(userId)
}

export async function deleteTodo(todoId: string, userId: string): Promise<null> {
  await todoAccess.deleteTodo(todoId, userId)
  return
}

export async function updateTodo(todoId: string, updatedTodo: UpdateTodoRequest, userId: string): Promise<null> {
  await todoAccess.updateTodo(todoId, updatedTodo, userId)
  return
}

export async function getPresignedUrl(todoId: string): Promise<string> {
  const todo = await todoAccess.getTodoById(todoId)
  todo.attachmentUrl = `https://${bucketName}.s3.amazonaws.com/${todoId}`

  await todoAccess.updateTodoAttachmentUrl(todo)

  return getUploadUrl(todoId)
}