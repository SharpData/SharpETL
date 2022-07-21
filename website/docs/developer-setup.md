---
title: Developer Setup
sidebar_position: 4
keywords: [ide, developer, setup]
toc: true
last_modified_at: 2019-12-30T15:59:57-04:00
---

## Pre-requisites

To contribute code, you need

 - a GitHub account
 - a Linux (or) macOS development environment with Java JDK 8 installed
 - [Docker](https://www.docker.com/) installed for running demo, integ tests or building website
 - docker compose


## IDE Setup

To contribute, you would need to do the following

- IntelliJ
- Scala plugin 

### Contributing Code

 - Once you finalize on a project/task, please open a new GitHub issue or assign an existing one to yourself. 
      - Almost all PRs should be linked to a GitHub issue. It's always good to have a GitHub issue upfront to avoid duplicating efforts.
      - But, you may be asked to file a GitHub issue, if reviewer deems it necessary
 - Before you begin work,
      - Claim the GitHub issue using the process above and assign the GitHub issue to yourself.
      - Click "Start Progress" on the GitHub issue, which tells everyone that you are working on the issue actively.
 - Make your code change
   - Get existing tests to pass using `./gradlew test`
   - Add adequate tests for your new functionality
   - For involved changes, it's best to test the changes in real production environments and report the results in the PR.
   - For website changes, please build the site locally & test navigation, formatting & links thoroughly
   - If your code change changes some aspect of documentation (e.g new config, default value change), 
     please ensure there is another PR to [update the docs](https://github.com/SharpData/SharpETL/tree/pages/README.md) as well.
 - Sending a Pull Request
   - Please ensure your commit message body is descriptive of the change. Bulleted summary would be appreciated.
   - Address code review comments & keep pushing changes to your fork/branch, which automatically updates the PR
   - Before your change can be merged, it should be squashed into a single commit for cleaner commit history.
 - Finally, once your pull request is merged, make sure to close issue.

### Coding guidelines 

Our code can benefit from contributors speaking the same "language" when authoring code. After all, it gets read a lot more than it gets
written. So optimizing for "reads" is a good goal. The list below is a set of guidelines, that contributors strive to upkeep and reflective 
of how we want to evolve our code in the future.

## Code & Project Structure

  * `common` : most common case classes
  * `core` : core mudules, like `WorkflowInterpreter`, `WorkflowParser` etc.
  * `datasource` : Generic datasource API
  * `docker` : Docker containers used by demo and integration tests. Brings up a mini data ecosystem locally
  * `flink` : Implementation for the Flink engine
  * `spark` : Implementation for the Spark engine

## Code WalkThrough

[This Quick start](/docs/quick-start-guide) will give you a start.

## Docker Setup

We encourage you to test your code on docker, please follow this for [docker setup](/docs/docker-setup).

## Website

[Sharp ETL site](/) is hosted on a special `pages` branch. Please follow the `README` file under `pages` on that branch for
instructions on making changes to the website.
