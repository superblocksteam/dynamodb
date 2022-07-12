import {
  camelCaseToDisplay,
  DatasourceMetadataDto,
  DynamoDBActionConfiguration,
  DynamoDBDatasourceConfiguration,
  ExecutionOutput,
  IntegrationError,
  RawRequest,
  TableType
} from '@superblocksteam/shared';
import { BasePlugin, PluginExecutionProps, safeJSONParse, getAwsClientConfig } from '@superblocksteam/shared-backend';
import { AWSError, DynamoDB } from 'aws-sdk';

export default class DynamoDBPlugin extends BasePlugin {
  async execute({
    context,
    datasourceConfiguration,
    actionConfiguration
  }: PluginExecutionProps<DynamoDBDatasourceConfiguration>): Promise<ExecutionOutput> {
    try {
      const ret = new ExecutionOutput();
      const dynamoDBClient = this.getDynamoDBClient(datasourceConfiguration);
      ret.output = await this.runAction(dynamoDBClient, actionConfiguration.action, actionConfiguration.body);
      return ret;
    } catch (err) {
      throw new IntegrationError(`DynamoDB request failed, ${err.message}`);
    }
  }

  dynamicProperties(): string[] {
    return ['action', 'body'];
  }

  escapeStringProperties(): string[] {
    return ['body'];
  }

  getRequest(actionConfiguration: DynamoDBActionConfiguration): RawRequest {
    const actionDisplayName = camelCaseToDisplay(actionConfiguration.action);
    return `Action: ${actionDisplayName}\n\nParams:\n${actionConfiguration.body}`;
  }

  async metadata(datasourceConfiguration: DynamoDBDatasourceConfiguration): Promise<DatasourceMetadataDto> {
    try {
      const dynamoDBClient = this.getDynamoDBClient(datasourceConfiguration);
      const data = await dynamoDBClient.listTables().promise();
      const tables =
        data.TableNames?.map((tableName: string) => {
          return {
            type: TableType.TABLE,
            name: tableName,
            columns: []
          };
        }) ?? [];
      return {
        dbSchema: {
          tables: tables
        }
      };
    } catch (err) {
      throw new IntegrationError(`DynamoDB listTables operation failed, ${err.message}`);
    }
  }

  private getDynamoDBClient(datasourceConfig: DynamoDBDatasourceConfiguration): DynamoDB {
    const dynamoDb = new DynamoDB(getAwsClientConfig(datasourceConfig));
    return dynamoDb;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private runAction(client: DynamoDB, action: string | undefined, params: any): Promise<any> {
    if (!action) {
      throw new IntegrationError(`No DynamoDB action specified`);
    }
    const fn = client[action];
    if (typeof fn !== 'function') {
      throw new IntegrationError(`Invalid DynamoDB action ${action}`);
    }

    const parsedParams = safeJSONParse(params, this.logger);

    return new Promise((resolve, reject) => {
      try {
        fn.apply(client, [
          parsedParams,
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (err: AWSError, data: any) => {
            if (!err) {
              resolve(data);
            } else {
              reject(err);
            }
          }
        ]);
      } catch (err) {
        reject(err);
      }
    });
  }

  async test(datasourceConfiguration: DynamoDBDatasourceConfiguration): Promise<void> {
    try {
      const dynamoDBClient = this.getDynamoDBClient(datasourceConfiguration);
      await dynamoDBClient.listTables().promise();
    } catch (err) {
      throw new IntegrationError(`DynamoDB listTables operation failed, ${err.message}`);
    }
  }
}
