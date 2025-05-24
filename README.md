<h1 align="center">
Marvelous MLOps End-to-end MLOps with Databricks course

## Week 1 Notes

With the Machine Learning experience being the bottleneck, I will be following the use case presented in the core-code-hub: **Real Estate Price Predicition**.
Databricks Bundles will be used from the start to gain a deeper understanding of their inner workings.

Additional first workflow has been created to download Kaggle House Pricing Dataset using `kaggle` API, followed by loading the table to Delta.
Here the compute resource is configured with additional spark ENV vars to faciliate authentication:

```
"KAGGLE_USERNAME": "{{secrets/personal/kaggle_username}}",
"KAGGLE_KEY": "{{secrets/personal/kaggle_key}}"
```



## Practical information
- Weekly lectures on Wednesdays 16:00-18:00 CET.
- Code for the lecture is shared before the lecture.
- Presentation and lecture materials are shared right after the lecture.
- Video of the lecture is uploaded within 24 hours after the lecture.

- Every week we set up a deliverable, and you implement it with your own dataset.
- To submit the deliverable, create a feature branch in that repository, and a PR to main branch. The code can be merged after we review & approve & CI pipeline runs successfully.
- The deliverables can be submitted with a delay (for example, lecture 1 & 2 together), but we expect you to finish all assignments for the course before the 15th of June.


## Set up your environment
In this course, we use Databricks 15.4 LTS runtime, which uses Python 3.11.
In our examples, we use UV. Check out the documentation on how to install it: https://docs.astral.sh/uv/getting-started/installation/

Install task: https://taskfile.dev/installation/

Update .env file with the following:
```
GIT_TOKEN=<your github PAT>
```

To create a new environment and create a lockfile, run:
```
task sync-dev
task venv-activate
```

Or, alternatively:
```
export GIT_TOKEN=<your github PAT>
uv venv -p 3.11 .venv
source .venv/bin/activate
uv sync --extra dev
```
