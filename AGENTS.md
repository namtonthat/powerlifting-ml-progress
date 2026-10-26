# AGENTS.md

## Caution: AWS credentials

The shell this repo is worked from often has a work `AWS_PROFILE` exported. A local step that calls AWS would silently use it.

- Always run local steps that touch AWS with this repo's `.env` credentials and `AWS_PROFILE` unset:
  `env -u AWS_PROFILE uv run --env-file .env/aws.env python steps/<step>.py`
- Personal AWS keys live in `.env/aws.env`, which is gitignored. Never put keys in `.env/project.env`: it is committed and this repo is public.
- To land data without local credentials, trigger the workflow instead: `gh workflow run <workflow>.yml -R namtonthat/powerlifting-ml-progress`.
