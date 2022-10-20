import * as AWS from 'aws-sdk'
const AWSXRay = require('aws-xray-sdk')
import { DocumentClient } from 'aws-sdk/clients/dynamodb'
import { createLogger } from '../utils/logger'
import { TodoItem } from '../models/TodoItem'
import { TodoUpdate } from '../models/TodoUpdate'

const XAWS = AWSXRay.captureAWS(AWS)

const logger = createLogger('TodosAccess')

// TODO: Implement the dataLayer logic
export class TodosAccess {

    constructor(
      private readonly docClient: DocumentClient = new XAWS.DynamoDB.DocumentClient(),
      private readonly todosTable = process.env.TODOS_TABLE,
      private readonly index = process.env.TODOS_CREATED_AT_INDEX) {
    }

    async createTodo(todo: TodoItem): Promise<TodoItem> {
        await this.docClient.put({
            TableName: this.todosTable,
            Item: todo
        }).promise()
    
        logger.info('Created new Todo in DynamoDB', { todo })
        return todo
    }
    
    async getAllTodosByUserId(userId: string): Promise<TodoItem[]> {
        const result = await this.docClient.query({
            TableName: this.todosTable,
            KeyConditionExpression: 'userId = :userId',
            ExpressionAttributeValues: {
              ':userId': userId
            }
        }).promise()
    
        logger.info(`Fetched all todos for user with Id of ${userId}`, { items: result.Items })
        return result.Items as TodoItem[]
    }
    
    async getTodoById(todoId: string): Promise<TodoItem> {
        const result = await this.docClient.query({
            TableName: this.todosTable,
            IndexName: this.index,
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
    
    async updateTodoAttachmentUrl(todo: TodoItem): Promise<TodoItem> {
        const result = await this.docClient.update({
            TableName: this.todosTable,
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
    
    async updateTodo(todoId: string, updatedTodo: TodoUpdate, userId: string): Promise<null> {
        const result = await this.docClient.update({
            TableName: this.todosTable,
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
    
    async deleteTodo(todoId: string, userId: string): Promise<null> {
        await this.docClient.delete({
            TableName: this.todosTable,
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
}

// function createDynamoDBClient() {
//     if (process.env.IS_OFFLINE) {
//         console.log('Creating a local DynamoDB instance')
//         return new XAWS.DynamoDB.DocumentClient({
//             region: 'localhost',
//             endpoint: 'http://localhost:8000'
//         })
//     }
  
//     return new XAWS.DynamoDB.DocumentClient()
// }