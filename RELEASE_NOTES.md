# Sinara Lib Release Notes
Sinara Lib is used as git submodule and in most updates versionless policy are used.

There are two correct options to use Sinara Lib in step component:
1. Use current version (for old steps where small changes needed)
2. Use most new version (for new steps or steps in active development)

Sinara Lib being actively evolving so new features that makes life better can bring breaking changes. Whese changes in most cases has small footprint. Each version description has link to the migration guide.

Following topics addresses these kind of changes.

## Version 1.1.2
1. Added loading of pipeline params and step params to the dedicated cell in the notebook.
2. Only substep params are defined in the 'parameters' cell and replaced by the job run.
3. Unnecessary default_param_values removed from the 'interface' cell.

[Migration Guide to the Sinara Lib version 1.1.2](https://github.com/4-DS/sinara/blob/main/SINARA_1.1.2_MIGRATION_GUIDE.md)
