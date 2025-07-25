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

### 1. Introduction (~1 page / 12.5%)
Hook: The growing demand for HPC across science and industry makes operational efficiency paramount. Every wasted CPU-hour or delayed computation represents a tangible cost.

Problem Statement: Two obstacles to improving HPC efficiency are (a) the opacity of scheduling systems and (b) the difficulty for administrators in identifying systemic resource waste. These problems persist due to a scarcity of comprehensive, public data linking job requests, scheduling, and actual performance.

Introducing the Solution (FRESCO): Briefly introduce the FRESCO dataset as the enabling resource for this study. Emphasize its unique combination of multi-institutional scale, long temporal range, and the crucial linkage of job accounting data with fine-grained performance metrics.

Research Questions (RQs):
1. What are the primary drivers of queue wait time in large-scale HPC systems, and can these wait times be accurately predicted based on a job's requested parameters? 
2. How prevalent and severe is resource waste (i.e., the over-requesting of resources), and do specific job characteristics or user behaviors correlate with higher levels of waste?

What to include:
- Summary of Contributions: State clearly what we will deliver:
  - A detailed characterization of the FRESCO dataset and its schema. 
  - An in-depth analysis of queue wait times, culminating in a predictive model for job scheduling delays. 
  - A novel methodology for quantifying and detecting resource waste from scheduler and performance logs. 
  - Actionable recommendations for HPC users and administrators based on our findings. 
- Roadmap: Briefly outline the structure of the rest of the paper.

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
