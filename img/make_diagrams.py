"""Create README.md overview diagram."""

# pylint: disable=expression-not-assigned

import os

from diagrams import Cluster, Diagram
from diagrams.aws.compute import Lambda
from diagrams.aws.integration import StepFunctions
from diagrams.aws.storage import S3
from diagrams.custom import Custom
from diagrams.gcp.analytics import Bigquery
from diagrams.onprem.ci import GithubActions
from diagrams.onprem.client import User
from diagrams.onprem.vcs import Git, Github
from diagrams.programming.language import Python


THIS_DIR = os.path.dirname(__file__)
ICONS_DIR = os.path.join(THIS_DIR, "icons")


with Diagram(
    "\nFlow",
    filename=os.path.join(THIS_DIR, "flow"),
    show=False,
    curvestyle="curved",
):
    gh_pr = Github("pull request to main")
    gh_merge = Github("merge to main")
    dbt = Custom("dbt build", os.path.join(ICONS_DIR, "dbt.png"))

    Git("push to dev") >> gh_pr
    gh_pr >> GithubActions("linting") >> Python("sqlfluff lint") >> gh_merge
    gh_merge >> GithubActions("deploy") >> dbt


with Diagram(
    "\nSchedule",
    filename=os.path.join(THIS_DIR, "schedule"),
    show=False,
    curvestyle="curved",
):
    dbt_run = Custom("dbt run --select +dim_player_last", os.path.join(ICONS_DIR, "dbt.png"))
    dbt_freshness = Custom("dbt source freshness", os.path.join(ICONS_DIR, "dbt.png"))

    GithubActions("every hour") >> dbt_run
    GithubActions("every day") >> dbt_freshness
