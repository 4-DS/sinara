# Sinara Lib Release Notes

## Version 1.1.2
This version has breaking changes. Pipelines than were made on previous verssions of the Sinara Lib are not compatible with current versuin and will not run properly.
1. Added loading of pipeline params and step params to the dedicated cell in the notebook.
2. Only substep params are defined in the 'parameters' cell and replaced by the job run.
3. Unnecessary default_param_values removed from the 'interface' cell.
