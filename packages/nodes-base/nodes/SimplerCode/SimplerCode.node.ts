/* eslint-disable n8n-nodes-base/node-filename-against-convention */
import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	IPairedItemData,
} from 'n8n-workflow';

export class SimplerCode implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Simpler Code',
		name: 'simplerCode',
		// eslint-disable-next-line n8n-nodes-base/node-class-description-icon-not-svg
		icon: 'file:simplercode.png',
		group: ['transform'],
		version: 1,
		description: 'Execute JavaScript code',
		defaults: {
			name: 'Simpler Code',
		},
		inputs: ['main'],
		outputs: ['main'],
		properties: [
			{
				displayName: 'JavaScript',
				name: 'jsCode',
				type: 'string',
				typeOptions: {
					editor: 'codeNodeEditor',
					editorLanguage: 'javaScript',
				},
				default: `
(dataList) => {
\s\sfor(let index = 0; index < dataList.length; index++) {
 \s\s\s\sdataList[index].codeModified = new Date();
\s\s}
\s\s
\s\s// expect an array as output
\s\sreturn dataList;
}`.trim(),
				description:
					'JavaScript code to execute.<br><br>Tip: You can use luxon vars like <code>$today</code> for dates and <code>$jmespath</code> for querying JSON structures. <a href="https://docs.n8n.io/nodes/n8n-nodes-base.function">Learn more</a>.',
				noDataExpression: true,
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][] | null> {
		const code = this.getNodeParameter('jsCode', 0) as string;
		const nodeContext = this.getContext('node');

		const returnItems: INodeExecutionData[] = [];
		const previousNode = this.getInputSourceData();
		nodeContext.previousNode = previousNode;
		const items = this.getInputData();
		if (nodeContext.isNotFirst === undefined) {
			// Is the first time the node runs

			nodeContext.userFunction = eval(code);
		}
		// The node has been called before. So return the next batch of items.
		nodeContext.isNotFirst = true;

		const result = await nodeContext.userFunction(items.map((item) => item.json));
		const dataResponse = result.map((item: any) => ({ json: item }));

		returnItems.push.apply(returnItems, dataResponse);

		const addSourceOverwrite = (pairedItem: IPairedItemData | number): IPairedItemData => {
			if (typeof pairedItem === 'number') {
				return {
					item: pairedItem,
					sourceOverwrite: nodeContext.previousNode,
				};
			}

			return {
				...pairedItem,
				sourceOverwrite: nodeContext.previousNode,
			};
		};

		function getPairedItemInformation(
			item: INodeExecutionData,
		): IPairedItemData | IPairedItemData[] {
			if (item.pairedItem === undefined) {
				return {
					item: 0,
					sourceOverwrite: nodeContext.previousNode,
				};
			}

			if (Array.isArray(item.pairedItem)) {
				return item.pairedItem.map(addSourceOverwrite);
			}

			return addSourceOverwrite(item.pairedItem);
		}

		returnItems.map((item) => {
			item.pairedItem = getPairedItemInformation(item);
		});

		return [returnItems];
	}
}
