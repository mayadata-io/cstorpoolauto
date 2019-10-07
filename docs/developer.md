# Development Workflow

## Prerequisites

- You have Go 1.12+ installed on your local host/development machine.
- You have Docker installed on your local host/development machine. 
    - Docker is required to build the container images

## Initial Setup

### Fork the project

1. Visit https://github.com/mayadata-io/cstorpoolauto
2. Click `Fork` button (top right)

### Clone fork to local host

Place mayadata-io/cstorpoolauto' code on your local machine.
Create your clone:

```sh

# cd to your code location
mkdir -p mayadata-io
cd mayadata-io

# Note: Here $user is your GitHub profile name
git clone https://github.com/$user/cstorpoolauto.git

# Configure remote upstream
cd cstorpoolauto
git remote add upstream https://github.com/mayadata-io/cstorpoolauto.git

# Never push to upstream master
git remote set-url --push upstream no_push

# Confirm that your remotes make sense
git remote -v
```

## Development

### Always sync your local repository

Open a terminal on your local host. Change directory to the fork root.

```sh
$ cd mayadata-io/cstorpoolauto
```

 Checkout the master branch.

 ```sh
 $ git checkout master
 Switched to branch 'master'
 Your branch is up-to-date with 'origin/master'.
 ```

 Recall that origin/master is a branch on your fork i.e. origin.
 Make sure you have the upstream remote mayadata-io/cstorpoolauto by listing them.

 ```sh
 $ git remote -v
 origin	https://github.com/$user/cstorpoolauto.git (fetch)
 origin	https://github.com/$user/cstorpoolauto.git (push)
 upstream	https://github.com/mayadata-io/cstorpoolauto.git (fetch)
 upstream	no_push (push)
 ```

 If the upstream is missing, add it by using below command.

 ```sh
 $ git remote add upstream https://github.com/mayadata-io/cstorpoolauto.git
 ```

 Fetch all the changes from the upstream master branch.

 ```sh
 # At master branch run following
 $ git fetch upstream master
 remote: Counting objects: 141, done.
 remote: Compressing objects: 100% (29/29), done.
 remote: Total 141 (delta 52), reused 46 (delta 46), pack-reused 66
 Receiving objects: 100% (141/141), 112.43 KiB | 0 bytes/s, done.
 Resolving deltas: 100% (79/79), done.
 From github.com:openebs/maya
   * branch            master     -> FETCH_HEAD
 ```

 Rebase your local master with the upstream/master.

 ```sh
 $ git rebase upstream/master
 First, rewinding head to replay your work on top of it...
 Fast-forwarded master to upstream/master.
 ```

 This command applies all the commits from the upstream master to your local master.

 Check the status of your local branch.

 ```sh
 $ git status
 On branch master
 Your branch is ahead of 'origin/master' by 38 commits.
 (use "git push" to publish your local commits)
 nothing to commit, working directory clean
 ```

 Your local repository now has all the changes from the upstream remote. You need to push the changes to your own remote fork which is origin master.

 Push the rebased master to origin master.

 ```sh
 $ git push origin master
 Username for 'https://github.com': $user
 Password for 'https://$user@github.com':
 Counting objects: 223, done.
 Compressing objects: 100% (38/38), done.
 Writing objects: 100% (69/69), 8.76 KiB | 0 bytes/s, done.
 Total 69 (delta 53), reused 47 (delta 31)
 To https://github.com/$user/maya.git
 8e107a9..5035fa1  master -> master
 ```

### Create a new feature branch to work on your issue

 Your branch name should have the format XX-descriptive where XX is the issue number you are working on followed by some descriptive text. For example:

 ```sh
 $ git checkout -b 1234-fix-developer-docs
 Switched to a new branch '1234-fix-developer-docs'
 ```

### Make your changes and build them

 ```sh
 make
 ```

### Test your changes

 ```sh
 # Run every unit test
 make test
 ```

### Keep your branch in sync

[Rebasing](https://git-scm.com/docs/git-rebase) is very important to keep your branch in sync with the changes being made by others and to avoid huge merge conflicts while raising your Pull Requests. You will always have to rebase before raising the PR.

```sh
# While on your myfeature branch (see above)
git fetch upstream
git rebase upstream/master
```

While you rebase your changes, you must resolve any conflicts that might arise and build and test your changes using the above steps.