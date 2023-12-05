You should bring your notebook (substep) to the view:

Example:
![image](https://github.com/4-DS/sinara/assets/71835176/460e005d-7f0f-496d-a7c5-bd22859fd354)

In order to make this, please, follow the steps below:

1. Remove **pipeline_params** and **step_params** . 
   ![image](https://github.com/4-DS/sinara/assets/71835176/f0410cfc-30df-407a-b0f1-8f5c92c12c0e)

   Leave only **substep_params** with the corresponding comment:

   ```
   # specify substep parameters for interactive run
   # this cell will be replaced during job run with the parameters from json within params subfolder
   substep_params={}
   ```

2. Add a cell with the code right after **substep_params** cell

   ```
   # load pipeline and step parameters - do not edit
   from sinara.substep import get_pipeline_params, get_step_params
   pipeline_params = get_pipeline_params(pprint=True)
   step_params = get_step_params(pprint=True)
   ```

3. Remove **default_params_values** from substep interface
   ![image](https://github.com/4-DS/sinara/assets/71835176/5a195fed-56bb-41a2-96c1-ac7a4c1e31fc)

   Example:
   
   ![image](https://github.com/4-DS/sinara/assets/71835176/a328361e-45d6-4db1-b767-cdc0269c2cf8)

4. (Optional) Bring the parameters that should be accessible across substeps (steps) into **step_params.json**

   Example:

   ![image](https://github.com/4-DS/sinara/assets/71835176/5ea1b0d3-7b79-48e9-ae04-7c8013acafd7)

   Hint: Here we declare **train_params**, which is needed across substeps of model_train step.
