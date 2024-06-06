/* eslint-disable n8n-nodes-base/node-filename-against-convention */

import vm from 'node:vm';

import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
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
for (let item of $input) {
  item.json.myNewField = 1;
}

return $input;`.trim(),
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
			const functionWrapper = `
        async function n8nOutsiderFunction() {
          ${code}
        }

        n8nOutsiderFunction().then(functionOutput => {
          output.response = functionOutput;
        });

      `;

			const scriptContext: vm.Context = {
				$input: items,
				output: {},
			};
			vm.createContext(scriptContext);

			nodeContext.userFunction = new vm.Script(functionWrapper);
			nodeContext.userContext = scriptContext;
			// vm.measureMemory
		}
		// The node has been called before. So return the next batch of items.
		nodeContext.isNotFirst = true;
		nodeContext.userContext.$input = items;

		// await nodeContext.userFunction.runInNewContext(scriptContext);
		// await nodeContext.userFunction.runInThisContext();
		await nodeContext.userFunction.runInContext(nodeContext.userContext);
		const dataResponse = nodeContext.userContext.output.response;

		returnItems.push.apply(returnItems, dataResponse);

		// global.gc();
		return [returnItems];
	}
}
