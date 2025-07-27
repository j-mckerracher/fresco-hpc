# Paper Name Ideas
- Wasted Cycles and Waiting Games: A Predictive Analysis of HPC Inefficiency using the FRESCO Dataset
- Data-Driven Insights into HPC Efficiency: Predicting Wait Times and Quantifying Resource Waste Across Multi-Institutional Systems
- Operational Intelligence for HPC Systems: Wait Time Prediction and Resource Waste Detection Using 20.9 Million Job Records
- Understanding HPC System Behavior Through Data: A Multi-Institutional Analysis of Job Scheduling and Resource Efficiency
- Predictive Modeling and Resource Waste Analysis in Production HPC Environments: Insights from the FRESCO Dataset
- Towards More Efficient HPC Operations: A Data-Driven Study of Queue Wait Times and CPU Utilization Patterns
- Applied Analytics for HPC Resource Management: Characterizing Job Performance and Detecting Inefficiencies at Scale
- Multi-Dimensional Analysis of HPC Job Characteristics: Predicting Scheduling Delays and Identifying Resource Underutilization

## Page count breakdown per topic:
1. Intro & Background: ~1.5 pages (19%)
2. Dataset Description & Schema: ~1 page (12.5%)
3. Analysis 1 (Wait Time): ~2.75 pages (34.5%)
4. Analysis 2 (Resource Waste): ~1.75 pages (22%)
5. Conclusion & Future Work: ~1 page (12%)

# 1. Introduction (~1 page / 12.5%)

**Hook:** High-Performance Computing systems represent billions of dollars in infrastructure investment, yet preliminary studies suggest that X-Y% of requested CPU cycles go unused while thousands of jobs queue for hours daily. This paradox—simultaneous resource scarcity and waste—undermines the scientific mission these systems were built to serve and represents a critical operational challenge as demand for computational resources continues to grow.

**Problem Statement:** At least two fundamental obstacles prevent HPC systems from achieving their efficiency potential. First, the opacity of job scheduling systems forces users to make resource requests in the dark. Without visibility into queue priorities, wait time factors, or historical patterns, users resort to defensive over-requesting—asking for more cores, memory, or time than needed as insurance against delays and failures. This creates a tragedy of the commons where individual rational behavior degrades system-wide performance. Second, administrators lack the tools to systematically identify and address resource waste patterns. While egregious cases of underutilization may be obvious, subtle but widespread inefficiencies remain hidden, and there are few established methodologies for quantifying waste or correlating it with user behaviors and job characteristics.

These problems persist because comprehensive, public datasets linking job requests, scheduling decisions, and actual resource consumption have been scarce. Without such data, both users and administrators operate on intuition rather than evidence, perpetuating cycles of inefficiency that could otherwise be broken through data-driven insights.

**Addressing the Gap with FRESCO:** The recently released FRESCO dataset provides an unprecedented opportunity to tackle these challenges systematically. FRESCO uniquely combines multi-institutional scale (20.9 million jobs across 75 months from three major academic computing centers), temporal depth, and—critically—the linkage of job accounting data with fine-grained performance metrics. This combination enables analyses that were previously extremely difficult: we can now examine not just what users requested, but what they actually used, and correlate both with waiting times and scheduling outcomes.

**Research Questions:** Building on this foundation, we address two key questions that directly target the problems identified above:

**RQ1:** What job characteristics drive queue wait times in large-scale HPC systems, and can we provide users with accurate wait time predictions to inform smarter resource requests and submission strategies?

**RQ2:** How prevalent and severe is resource waste across different job types and user behaviors, and can we develop systematic approaches to detect and quantify underutilization patterns that administrators can act upon?

Answering these questions has implications beyond operational efficiency. More effective HPC resource utilization can accelerate scientific discovery, reduce barriers to computational research for resource-constrained groups, and minimize the environmental impact of underutilized infrastructure.

**Summary of Contributions:** This work delivers four key contributions:
- A detailed characterization of the FRESCO dataset and its schema, establishing its value for HPC operational research
- An in-depth analysis of queue wait times across multiple institutions, culminating in a predictive model that provides actionable insights for both users and administrators
- A novel methodology for quantifying and detecting resource waste from scheduler and performance logs, with validated metrics that distinguish between different types of underutilization
- Concrete, data-driven recommendations for HPC users seeking to optimize their resource requests and administrators aiming to improve system-wide efficiency

**Roadmap:** The remainder of this paper is structured as follows. Section 2 provides background on HPC scheduling and situates our work within existing research. Section 3 introduces the FRESCO dataset and its multi-institutional scope, while Section 4 details the data schema and key metrics used in our analyses. Sections 5 and 6 present our wait time prediction and resource waste detection analyses, respectively. We conclude in Section 7 with a discussion of broader implications and future research directions.

### 2. Background and Related Work (~0.5 pages / 6.5%)
HPC Scheduling and Resource Management: Briefly explain the role of schedulers (e.g., Slurm, PBS/TORQUE) and the concept of job queues.

What to include:
- The Challenge of HPC Data: Discuss the historical lack of public HPC operational data. 
- Existing Datasets: Mention prior work like the Google Cluster Trace, CFDR, and the Atlas project. Use the FRESCO paper (Figure 1/Table 1) to position FRESCO as a significant advancement, specifically highlighting its inclusion of fine-grained performance metrics (value_* fields) which enables the resource waste analysis in a way previous datasets could not.

## Part I: The FRESCO Dataset (Total: ~1 page / 12.5%)
### 3. The FRESCO Resource: A Multi-Institutional View of HPC Operations
Overview: Describe FRESCO's scope: 20.9M jobs, 75 months, 3 institutions (Purdue Anvil, Purdue Conte, TACC Stampede).

What to include:
- System Diversity: Detail the characteristics of the encompassed systems (Table 1 from the FRESCO paper). Explain why this diversity (different architectures, schedulers, and user communities) makes the dataset robust and our findings more generalizable. 
  - Data Collection and Integration: Briefly summarize the methodology from the FRESCO paper (Section 3.2 & 4) to establish the data's credibility. Highlight the process of linking job accounting data with performance metrics as the key technical enabler for this work.

### 4. FRESCO Data Schema and Semantics
Objective: To provide a clear understanding of the data fields used in our analysis.

#### What to include:

Job Submission and Execution Lifecycle:
- Use a diagram to illustrate a job's lifecycle: submit_time -> start_time -> end_time.
- Explain key scheduler fields: timelimit, nhosts, ncores, queue, account.

Job Outcome Analysis:
- Explain the exitcode field and how we will map these codes to standardized states: COMPLETED, FAILED, TIMEOUT, CANCELLED. This is crucial for filtering and analysis.

Performance Metrics for Waste Detection:
- Introduce the key metrics: value_cpuuser, value_memused. Explain what they represent (e.g., value_cpuuser as a measure of actual CPU cycles consumed).

## Part II: Data-Driven Analyses of HPC Efficiency (Total: ~4.5 pages / 56.5%)
### 5. Analysis 1: Characterizing and Predicting Queue Wait Time (~2.75 pages / 34.5%)
#### 5.1. Methodology

Defining Wait Time: Formally define the target variable: WaitTime=start_time−submit_time.
- Data Preparation: Describe filtering steps (e.g., focusing only on jobs that eventually ran, handling outliers or anomalous timestamps).

Explanatory Variables: List the features from the FRESCO schema to be used: ncores, timelimit, queue, time-of-day/day-of-week (engineered from submit_time), and potentially system load (proxy variable).

Predictive Modeling:
Model Choice: Justify the model choice
Training & Evaluation: Detail the evaluation strategy.

#### 5.2. Results and Discussion

Descriptive Statistics: Present distributions of wait times across different queues and clusters.
Correlations: Show the relationship between wait time and requested resources (ncores, timelimit). Visualize this with scatter plots or heatmaps. Hypothesis: Is the relationship linear, or do very large/very small jobs face disproportionately long waits?
Predictive Performance: Report metrics for the trained model. Compare to a simple baseline (e.g., predicting the mean wait time for a given queue).
Feature Importance: Present a feature importance plot from the model. Discuss the top drivers of wait time.

#### 5.3. Practical Implications

For users: Provide data-driven advice.
For administrators: Suggest potential policy adjustments.

### 6. Analysis 2: Detecting and Quantifying Resource Waste (~1.75 pages / 22%)
#### 6.1. Methodology

Defining Resource Waste:
- Propose a "CPU Waste Ratio" metric?
- Crucial Distinction: Differentiate between "single-process waste" (requesting N cores but only using 1, Waste Ratio ≈ 1−1/N) and "multi-process underutilization"
- Data Cohorts: Define cohorts for analysis: single-node jobs, multi-node jobs, jobs by queue, jobs by exitcode (do failed jobs show higher waste?).

#### 6.2. Results and Discussion

Prevalence of Waste: Present the overall distribution of the CPU Waste Ratio across the dataset.
Waste by Job Scale: Plot the average Waste Ratio against the number of requested cores (ncores). **Hypothesis: Waste increases with the number of cores requested.**
Waste vs. Job Outcome: Compare the waste distributions for COMPLETED vs. FAILED vs. TIMEOUT jobs. Are failing jobs more wasteful, perhaps indicating a misconfigured application?
Identifying Archetypes: Use examples to illustrate findings. Show a "hero" job (low waste) and a "wasteful" job (high waste) with their respective parameters and performance metrics.

#### 6.3. Practical Implications

For users: Highlight the cost of over-provisioning and provide guidance on how to use tools to better profile their applications before requesting large allocations.
For administrators: Suggest methods for flagging potentially wasteful jobs. This could lead to a system that proactively contacts users whose jobs consistently show high waste ratios, offering support and guidance.

### 7. Conclusion and Future Work (~1 page / 12%)

Summary of Findings: Reiterate the key answers to the research questions (RQ1 and RQ2). Emphasize the practical, data-driven nature of our conclusions.
Broader Impact: Frame our work as a significant step toward more transparent, efficient, and data-driven management of large-scale computing infrastructure, directly aligning with the goals of the Applied Data Science track.
Limitations: Acknowledge any limitations. For example, the value_cpuuser metric doesn't capture I/O-bound idleness perfectly. The dataset, while large, does not represent all HPC workloads (e.g., commercial).
Future Work: ?

### 8. References (Up to 2 pages, not counted in the 8-page limit)
[List of cited works, including the FRESCO paper, Atlas, etc.]
