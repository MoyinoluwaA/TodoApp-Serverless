import { APIGatewayProxyEvent } from 'aws-lambda';
import { TodoItem } from '../models/TodoItem'
import { CreateTodoRequest } from '../requests/CreateTodoRequest'
import { createLogger } from '../utils/logger'
import * as uuid from 'uuid'
import { getUserId } from '../lambda/utils';

// // TODO: Implement businessLogic
export function todoBuilder(todoRequest: CreateTodoRequest, event: APIGatewayProxyEvent): TodoItem {
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
    return todo as TodoItem
}