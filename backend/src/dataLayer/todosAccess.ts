import * as AWS from 'aws-sdk'
const AWSXRay = require('aws-xray-sdk')
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { createLogger } from '../utils/logger'
import { TodoItem } from '../models/TodoItem'
import { TodoUpdate } from '../models/TodoUpdate';

const XAWS = AWSXRay.captureAWS(AWS)

const logger = createLogger('TodosAccess')
const todosTable = process.env.TODOS_TABLE;
const index = process.env.TODOS_CREATED_AT_INDEX;
const docClient: DocumentClient = createDynamoDBClient();

// TODO: Implement the dataLayer logic
export async function createTodo(todo: TodoItem): Promise<TodoItem> {
    await docClient.put({
        TableName: todosTable,
        Item: todo
    }).promise()

    logger.info('Created new Todo in DynamoDB', { todo })
    return todo
}

export async function getAllTodosByUserId(userId: string): Promise<TodoItem[]> {
    const result = await docClient.query({
        TableName: todosTable,
        KeyConditionExpression: 'userId = :userId',
        ExpressionAttributeValues: {
          ':userId': userId
        }
    }).promise()

    logger.info(`Fetched all todos for user with Id of ${userId}`, { items: result.Items })
    return result.Items as TodoItem[]
}

export async function getTodoById(todoId: string): Promise<TodoItem> {
    const result = await docClient.query({
        TableName: todosTable,
        IndexName: index,
        KeyConditionExpression: 'todoId = :todoId',
        ExpressionAttributeValues: {
          ':todoId': todoId
        }
    }).promise()

    const items = result.Items
    if (items.length !== 0) return items[0] as TodoItem

    logger.info(`Fetched a todo by id`, { items: items.length !== 0 ? items[0] : null })

    return null
}

export async function updateTodoAttachmentUrl(todo: TodoItem): Promise<TodoItem> {
    const result = await docClient.update({
        TableName: todosTable,
        Key: {
            userId: todo.userId,
            todoId: todo.todoId
        },
        UpdateExpression: 'set attachmentUrl = :attachmentUrl',
        ExpressionAttributeValues: {
          ':attachmentUrl': todo.attachmentUrl
        }
    }).promise()

    logger.info(`Updating attachment url for todo with id of ${todo.todoId} and userId ${todo.userId}`, { 
        updatedTodo: result.Attributes 
    })
    return result.Attributes as TodoItem
}

export async function updateTodo(todoId: string, updatedTodo: TodoUpdate, userId: string): Promise<TodoItem> {
    const result = await docClient.update({
        TableName: todosTable,
        Key: {
            userId: userId,
            todoId: todoId
        },
        UpdateExpression: 'set #todoName = :name, #todoDueDate = :dueDate, #todoDone = :done',
        ExpressionAttributeValues: {
          ':name': updatedTodo.name,
          ':dueDate': updatedTodo.dueDate,
          ':done': updatedTodo.done
        },
        ExpressionAttributeNames: {
            '#todoName': 'name',
            '#todoDueDate': 'dueDate',
            '#todoDone': 'done'
        }
    }).promise()

    logger.info(`Updating todo with id of ${todoId} and userId ${userId}`, {
        updatedTodo: result.Attributes
    })

    return
}

export async function deleteTodo(todoId: string, userId: string): Promise<null> {
    await docClient.delete({
        TableName: todosTable,
        Key: {
            todoId,
            userId: userId
        }
    }).promise()
    logger.info(`Deleting todo with id of ${todoId} and userId ${userId}`, { 
        deletedTodoId: todoId
    })

    return null
}

function createDynamoDBClient() {
    if (process.env.IS_OFFLINE) {
        console.log('Creating a local DynamoDB instance')
        return new XAWS.DynamoDB.DocumentClient({
            region: 'localhost',
            endpoint: 'http://localhost:8000'
        })
    }
  
    return new XAWS.DynamoDB.DocumentClient()
}