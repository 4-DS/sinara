Go to your each notebook (substep) of your each step and do the following:

1. Remove **pipeline_params** and **step_params** . 
![image](https://github.com/4-DS/sinara/assets/71835176/f0410cfc-30df-407a-b0f1-8f5c92c12c0e)

Leave only **substep_params** with the corresponding comment:

```
# specify substep parameters for interactive run
# this cell will be replaced during job run with the parameters from json within params subfolder
substep_params={}
```

Example:
![image](https://github.com/4-DS/sinara/assets/71835176/94384f05-f924-4db0-95b5-c5dca3e222c2)



2. Remove **default_params_values** from substep interface
![image](https://github.com/4-DS/sinara/assets/71835176/5a195fed-56bb-41a2-96c1-ac7a4c1e31fc)

Example:

![image](https://github.com/4-DS/sinara/assets/71835176/a328361e-45d6-4db1-b767-cdc0269c2cf8)

3. Bring the parameters that should be accessible across substeps (steps) into **step_params.json**

   Example:

   ![image](https://github.com/4-DS/sinara/assets/71835176/5ea1b0d3-7b79-48e9-ae04-7c8013acafd7)
