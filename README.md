# Mobile Fraud Detect Solution - Identifying Fraud Attempts in Mobile Applications

## I. Case Summary and Objective

Fraud in financial transaction application logins has become increasingly sophisticated, especially on mobile devices. One of the most common practices is Account Takeover (ATO), which occurs when attackers gain unauthorized access to a legitimate account, often using social engineering techniques, phishing, or through the most basic form of this type of attack, which involves bot-driven brute force attacks that send random combinations of characters to login forms until they find the account's credential combination.

In the context of a financial transaction application, such as the one developed in this case, attackers can use personal information (such as CPF, IMEI, MAC address, or location data) to simulate that they are using an authenticated device. Through login with stolen credentials, they attempt to execute fraudulent transactions, transferring funds or accessing sensitive information.

Behavior patterns are crucial for detecting account takeover. Among the warning signs are:

-	Repeated logins from multiple unknown devices.
-	Frequent changes of geographic locations.
-	Use of devices different from the usual for a specific CPF.
-	Discrepancies between the operating system version or the application version used in the logins.
-	Multiple failed login attempts in a short time interval.

The solution proposed in this case uses a robust real-time data ingestion flow, where login and device information is sent to a cloud data lake, which can feed fraud prevention and detection teams. With this data in hand, it is possible to identify suspicious patterns and behaviors in historical data to extract insights that can expose fraud attempts and serve auditing processes for possible successful ATO cases.

Thus, this work aims to implement a practical case of collecting access logs from a financial transaction application, where the architecture contains data ingestion, transformation, and visualization in a cloud data lake (Azure Data Lake Gen2).

## II. Solution Architecture and Technical Architecture

There are several strategies to mitigate and solve fraud attempt problems in Mobile and Web systems, from user awareness strategies to Anti-Bot solutions. To choose and evaluate the most assertive strategy, it is important that the risk and fraud departments are able to identify and understand the main characteristics of the imminent danger. The collection and evaluation of application logs, as illustrated in Figure 1, with data solutions assists the fraud professional in this flow of choosing protection and improvement strategies.

<p align="center">
  <img src="Editaveis/Imagens/mobile-fraud-detect-funct.jpeg" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 1: Functional architecture for data ingestion and transformation for decision making</em>
</p>

Figure 2 presents the technical architecture for ingesting and transforming data collected from a mobile application login system, implemented as a Data Lakehouse solution on Azure, organized according to the principles of the medallion architecture.

    Data Lakehouse and Medallion Architecture
  
The Data Lakehouse concept combines the scalability and flexibility of a Data Lake with the structure and performance of a Data Warehouse, allowing the storage of large volumes of data in their raw format, while supporting optimized analytical queries directly over the data.

The [medallion architecture](https://learn.microsoft.com/pt-br/azure/databricks/lakehouse/medallion) organizes the pipeline into logical layers:

- `Bronze`: Stores raw data in its original format, such as access logs and login system events, preserving the integrity of the collected data.
- `Silver`: Contains pre-processed and cleaned data, where redundant or inconsistent information is handled, allowing for more efficient analysis.
- `Gold`: Represents the most refined layer, with transformed data ready for advanced analysis, such as detecting fraud attempts in logins.

This approach improves governance, optimizes query performance, and enables the use of advanced big data tools, such as Apache Spark and Databricks, to meet analytical requirements.

<p align="center">
  <img src="Editaveis/Imagens/mobile-fraud-detect-V1.jpeg" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 2: Architecture for data ingestion and transformation in a data lake on Azure</em>
</p>

The presented technical architecture consists of the following components:

[Azure Event Hub](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-about) is a highly scalable real-time event processing and data ingestion platform, ideal for collecting and processing large volumes of data from IoT devices, application logs, or other systems. It acts as an event broker that allows the ingestion and retention of messages, making them available to consumers in near real-time or with batch processing.

For this case, the Event Hub with the Basic SKU was chosen, as it offers the lowest cost among the available plans. However, the Basic SKU has some important limitations, such as the absence of the Capture feature, which facilitates the automatic recording of events to storage for later processing, and a retention period limited to one day. Given this, to ensure that no message is lost due to the short retention interval, a solution based on Spark Streaming was adopted. This approach enables the continuous reading of Event Hub events and the writing of data directly to the Bronze layer of the data lake, allowing the ingestion pipeline to be reliable and resilient to losses.

[Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/introduction/) is a unified analytics and data science platform based on Apache Spark, designed to simplify large-scale data processing, machine learning, and data engineering. It combines distributed processing capabilities with a collaborative environment, allowing data teams to develop, test, and scale pipelines efficiently.

In this project, Databricks is the tool used in the processing layer, performing data ingestion and transformation, ensuring three fundamental pillars:

- Security: The platform offers native integration with tools such as Azure Key Vault for secure credential management and support for security policies that protect sensitive user data.
- Scalability: Designed to handle large volumes of data, it automatically adjusts computational resources to meet variable demands, such as the usage peaks common in financial or transactional applications.
- Efficiency: With features such as Adaptive Query Execution (AQE), which optimizes execution plans dynamically; Optimization, which reduces latencies; and Z-Order, which organizes data for faster access, Databricks helps optimize costs and improve query performance at scale.

An essential feature for security in Databricks is the secret scope, which allows creating secure areas in the workspace to store credentials such as keys, passwords, or tokens. This functionality eliminates the need to expose sensitive information in the code, ensuring compliance with security best practices. In addition, the integration with Azure Key Vault automates the synchronization and management of these secrets, offering an additional level of protection.

For storage, [Azure Data Lake Storage Gen2 (ADLS Gen2)](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction) was adopted, due to its high capacity and native integration with the Hadoop Distributed File System (HDFS), which facilitates interaction with big data tools, such as Apache Spark and Databricks. ADLS Gen2 also offers hierarchical storage, which improves performance when accessing large volumes of data organized in directories, being especially useful in data architectures based on the medallion model. In addition, it supports data encryption with customer-managed keys, reinforcing security and meeting corporate compliance requirements.

In order to ensure a proper authorization flow between the solution components, [Service Principals](https://learn.microsoft.com/en-us/entra/identity-platform/app-objects-and-service-principals?tabs=browser) were used, which are secure identities managed in Azure Active Directory (AAD) and used for authentication and authorization in Azure resources. These identities allow applications, scripts, or automations to access Azure services with specific permissions, without the need to use user credentials.

Their importance lies in enabling centralized permission management, increasing security by avoiding the use of sensitive credentials directly in the code, in addition to enabling the implementation of granular access policies between services, such as Databricks, Event Hub, and ADLS Gen2. This ensures that each component has only the privileges necessary for its operation, reducing risks and meeting security best practices.

Finally, as a monitoring strategy, [Azure Monitor](https://learn.microsoft.com/pt-br/azure/azure-monitor/overview) was used for simplicity, as it is a native Azure tool, requiring no major configurations to collect metrics on the solution's operation.

## III. Explanation of the Developed Case

As explained earlier, the use of a streaming solution is essential to avoid the loss of messages due to expiration in the Event Hub. The Silver and Gold layers, on the other hand, are processed in batch flows, with jobs configured for daily execution, ensuring the balance between real-time processing and more refined analyses.

To replicate the infrastructure of this solution, it is necessary to have a subscription with the Azure cloud provider, in addition to creating a resource group and provisioning the following services:

- Event Hub Namespace (Basic SKU) for event ingestion;
- Azure Key Vault (Standard SKU) for secure storage of secrets;
- Storage Account with hierarchical namespace enabled, for organizing data in the Data Lake format;
- Databricks Workspace Premium, which offers advanced support for integrations and security.

To simplify the provisioning of the infrastructure, I provide the [script.sh](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/Infraestrutura/script.sh), which automates the creation of the mentioned resources.

### _Databricks Cluster and Scope_

As described in the architecture section, it is necessary to create a cluster. Below is the JSON provided to facilitate provisioning. Simply replace [my-storage-account-name] with the name of the provisioned ADLS resource, [my-spn-client-id] with the *client id* of the SPN created in AAD, and [my-tenant-id] with the *tenant id* of the subscription.

``` JSON
{
    "cluster_name": "fraud-cluster",
    "spark_version": "13.3.x-scala2.12",
    "spark_conf": {
        "spark.hadoop.fs.azure.account.oauth2.client.secret.[my-storage-account-name].dfs.core.windows.net": "{{secrets/dbwsscope/spn-secret}}",
        "spark.hadoop.fs.azure.account.oauth2.client.id.[my-storage-account-name].dfs.core.windows.net": "[my-spn-client-id]",
        "spark.hadoop.fs.azure.account.auth.type.[my-storage-account-name].dfs.core.windows.net": "OAuth",
        "spark.hadoop.fs.azure.account.oauth.provider.type.[my-storage-account-name].dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "spark.hadoop.fs.azure.account.oauth2.client.endpoint.[my-storage-account-name].dfs.core.windows.net": "https://login.microsoftonline.com/[my-tenant-id]/oauth2/token"
    },
    "azure_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK_AZURE",
        "spot_bid_max_price": -1
    },
    "node_type_id": "Standard_D4s_v3",
    "driver_node_type_id": "Standard_D4s_v3",
    "autotermination_minutes": 30,
    "enable_elastic_disk": true,
    "enable_local_disk_encryption": false,
    "runtime_engine": "STANDARD",
    "effective_spark_version": "11.3.x-scala2.12",
    "autoscale": {
        "min_workers": 2,
        "max_workers": 2
    },
    "apply_policy_default_values": false
}
```

As the spark configurations reference a variable to consume the secret from the Databricks Scope, it is necessary to perform a password ceremony to insert the secret of the *Service Principal* created in AKV and synchronize it with the scope created in Databricks. The creation and synchronization process can be consulted in the [Azure documentation](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/).

For the cluster, the Databricks Runtime version [13.3](https://learn.microsoft.com/pt-br/azure/databricks/release-notes/runtime/13.3lts) was used, which includes version 3.4.1 of Apache Spark, being a robust and stable version.

### _`Bronze` Ingestion_

The entire development of this work focuses on the data ingestion and transformation solution, addressing and using Data Engineering techniques. Thus, the development of data ingestion into the Event Hub was implemented through a Python notebook that generates and sends the data to the messaging broker through the AMQP protocol, using a Service Principal with a *role* of sending data only, where the notebook developed for this solution is found in [loginAuthenticationMockData.ipyn](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/dev-notebooks/0%20-%20mockData/loginAuthenticationMockData.ipynb). The process of sending data to the Event Hub topic is possible through the use of the [`azure-identity`](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python) package to authorize the component through the *SPN* spn-prdcr and [`azure-eventhub`](https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-python-get-started-send?tabs=passwordless%2Croles-azure-portal) to communicate with the resource, allowing the sending of information using *batches* of events produced by the Python component.

In order to read the messages from the topic and write to a `Delta Table` in *ADLS*, as presented in Figure 3, the streaming solution performs a reading flow from the Event Hub using the [`azure-event-hubs-spark`](https://github.com/Azure/azure-event-hubs-spark) package, which simplifies the connection of Spark with the Event Hub, requiring the installation of the package on the cluster provisioned in Databricks. One of the disadvantages of using this connector is that there is no support for the message authorization process with AAD through the SPN in a simple way. The documentation presents a way to perform authentication via AAD with an adaptation through the creation of a callback class developed in Scala. For more details, follow the [link](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/use-aad-authentication-to-connect-eventhubs.md), but to simplify the case and reduce development to a single programming language, authorization through the Event Hub *Connection String* stored in AKV was chosen, synchronizing the secret with the Databricks *Scope*.

<p align="center">
  <img src="Editaveis/Imagens/eventhubstreamingingestion.png" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 3: Reading the Event Hub messages and generating a Delta Table in ADLS Gen2</em>
</p>

To ensure security over sensitive data, an information anonymization strategy was used at the time of ingestion into the `Bronze` layer. A SHA-256 hash was created on the IMEI, MAC, CPF, and user Password information, concatenating these with a keyword stored in the Databricks *Scope* and retrieved at runtime.

Finally, to write the data, a 2-minute processing window was configured in Spark Streaming in the *Storage Account* to create the data frame and consequently the parquet file in a Delta Table. More details about the ingestion job can be found by looking at the code developed in the [dataStreamingLoad.ipynb](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/dev-notebooks/1%20-%20eventhubToBronzeStreaming/dataStreamingLoad.ipynb) notebook.

### _`Silver` Table_

The process of creating the `Silver` table, presented in Figure 4, involves the normalization of the ingested data, with the objective of correcting possible inconsistencies generated during ingestion and thus improving the quality of the analyses. During the transformation of data from the Bronze layer, the conversion of technical information into more functional data also occurs.

As the ultimate goal is to provide inputs for fraud-fighting professionals, who are more focused on behavior patterns that indicate risk, technical columns such as error, API, and endpoint are translated into simpler and more accessible information, facilitating the identification of risk profiles by teams with less technical expertise.

<p align="center">
  <img src="Editaveis/Imagens/silverjobtransformer.png" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 4: Data transformation job for generating a Silver layer</em>
</p>

The data transformation job for the `Silver` layer was developed with a focus on daily execution and incrementing the already existing table. For more details about the job implementation, you can consult the [dataJobClean.ipynb](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/dev-notebooks/2%20-%20silverBatch/dataJobClean.ipynb) notebook, which describes the complete flow and the operations performed.

### _`Gold` Table_

To generate highly aggregated data with information that facilitates fraud detection, a *Feature Engineering* method was adopted in the creation of the `Gold` table, presented in Figure 5. In this layer, the solution involves the creation of a table with specific columns for aggregation by user, including metrics such as: number of access attempts, successful accesses, number of devices used, diversity of networks accessed, count of passwords used, number of devices enabled for transactions, distinct number of application versions used, total of distinct locations, and a *flag* column indicating the fraud risk. These aggregated variables allow for more precise analyses and the identification of suspicious patterns that may indicate fraudulent activities.

<p align="center">
  <img src="Editaveis/Imagens/goldjobtransformer.png" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 5: Data transformation job for generating a Gold layer with business value</em>
</p>

The generation of the risk indicator was based on simple but effective rules, which consider factors such as the number of accesses performed, devices enabled for transactions, variety of passwords attempted, and the locations where the accesses occurred on the same day. These rules were strategically chosen for their ability to identify unusual behaviors that may be indicative of risk. The sample of the generated table, illustrated in Figure 6, demonstrates how these metrics are organized for analysis.

This approach is a good strategy because it combines simplicity with effectiveness. Although the rules are simple, they are highly interpretable and easy to implement, allowing for the rapid identification of anomalous patterns without the need for complex models. In addition, the analysis of multiple factors (such as devices, passwords, and locations) in a single indicator provides a more robust and granular view of user behavior, helping to detect fraud attempts with greater precision. The use of quantitative metrics also facilitates the definition of risk thresholds, making the detection process more agile and transparent.

<p align="center">
  <img src="Editaveis/Imagens/goldtable.png" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 6: Sample of the Gold table aggregated by user with quantitative figures of the user's accesses during the day</em>
</p>

More details about the process of creating the Gold table can be found in the [dataJobUserAgg.ipynb](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/dev-notebooks/3%20-%20goldBatch/dataJobUserAgg.ipynb) notebook.

### _Partitioning and Optimization_

Table partitioning in Databricks is an essential technique for improving read performance and managing large volumes of data. By partitioning a table, the data is divided into subfolders based on one or more columns, such as dates or locations, which allows only the relevant partitions to be read during queries, optimizing performance. However, according to the Databricks [documentation](https://docs.databricks.com/pt/tables/partitions.html), it is recommended to avoid partitioning tables smaller than 1TB, as the cost of managing many small partitions can outweigh the performance benefits. For this reason, in this solution, partitioning was not used, as the size of the partitions depends directly on the TPS (Transactions Per Second) of the application's login solution, which may not justify partitioning with the current size of the data.

Without table partitioning, the [`OPTIMIZE`](https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/delta-optimize) command in Databricks is used to improve the read performance of a Delta table, performing the compaction of small files into larger partitions. This is especially useful when you have a table with many partitions or small files, which can generate overhead during data reading. `OPTIMIZE` physically reorganizes the data, reducing the number of files and increasing the efficiency of subsequent queries. For this case, two jobs were developed to optimize the parquet files generated in each table, which can be verified in the [optimizeSilverDeltaTable.ipynb](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/dev-notebooks/4%20-%20optimize/optimizeSilverDeltaTable.ipynb) and [optimizeGoldDeltaTable.ipynb](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/dev-notebooks/4%20-%20optimize/optimizeGoldDeltaTable.ipynb) notebooks.

### _Data Visualization Example_

As a suggestion for a dashboard for quick identification of events and users with fraud risks, a report was built with *Power BI*, illustrated in Figure 7, Figure 8, and Figure 9. The *`PBIX`* file is available at the [link](https://github.com/Foiac/MobileFraudDetectSolution/tree/main/Editaveis/PBI). To establish the connection with Azure Databricks, it was necessary to configure the connector in Power BI by inserting the *Access Token* generated via the Databricks Workspace and the cluster's JDBC information. Tutorial available [here](https://docs.databricks.com/pt/partners/bi/power-bi.html#connect-power-bi-desktop-to-databricks)

<p align="center">
  <img src="Editaveis/Imagens/home-dash.jpeg" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 7: Dashboard home with presentation information and description of the metrics</em>
</p>

<p align="center">
  <img src="Editaveis/Imagens/indicadores-dash.jpeg" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 8: Indicators screen</em>
</p>

<p align="center">
  <img src="Editaveis/Imagens/analitico-dash.jpeg" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 9: Analytics for evaluating events by user</em>
</p>

### _Monitoring_

As a monitoring strategy for the solution, the use of Azure Monitor was adopted with the construction of a dashboard for monitoring the data ingestion process into the topic and the process of reading and persisting the data in the `Bronze` layer, illustrated in Figure 10 and the file for import and replication available in the [json](https://github.com/Foiac/MobileFraudDetectSolution/blob/main/Editaveis/Monitoramento/Fraud%20Analytics%20Solution%20Azure%20Monitor.json).

<p align="center">
  <img src="Editaveis/Imagens/monitoramento.jpeg" alt="Technical Architecture" width="1100">
  <br>
  <em>Figure 10: Solution monitoring dashboard</em>
</p>

The dashboard provides some basic information to detect possible problems when performing data ingestion, or bottlenecks in the data pipeline. To perform *troubleshooting*, simply click on the resource on the right side of the dashboard and investigate its metrics and logs in more depth. For the spark metrics, it is also important to access the cluster's metrics and logs to understand where the root cause of a possible problem arises.

## IV. Improvements and Final Considerations

The proposed solution represents a robust foundation for fraud detection in mobile applications, demonstrating how Data Lakehouse architectures can be applied to complex problems. However, there are several opportunities to expand and optimize the architecture, increasing its performance and broadening its applicability in real production scenarios.

One of the most relevant improvements would be to expand the solution to address more complex financial transaction scenarios, such as anti-money laundering (AML) prevention. This would involve creating algorithms that detect atypical transfer patterns, also enabling the cross-referencing of information with external databases, such as sanctions or suspect lists. This approach would allow not only identifying fraud but also ensuring compliance with financial regulations.

Another evolution would be the integration of Machine Learning techniques to expand the analytical power of the solution. Models such as Isolation Forest, DBSCAN, or K-Means can identify anomalous patterns in login and transaction data. CI/CD pipelines would be indispensable to automate provisioning and deployment, increasing agility in delivering improvements and new features.

Finally, to run the solution in a production environment, it is essential to orchestrate the data ingestion, transformation, and optimization processes in an automated way, using tools such as Databricks Workflows or Azure Data Factory to ensure consistency and efficiency. Cluster management policies in Databricks and data purge jobs also play a crucial role, helping to control costs, reinforce security, and avoid inadequate configurations that could compromise the operation.

