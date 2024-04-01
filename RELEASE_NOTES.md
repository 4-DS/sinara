# Sinara Lib Release Notes
Sinara Lib is used as git submodule and in most updates versionless policy are used.

There are two correct options to use Sinara Lib in step component:
1. Use current version (for old steps where small changes needed)
2. Use most new version (for new steps or steps in active development)

Sinara Lib being actively evolving so new features that makes life better can bring breaking changes. Whese changes in most cases has small footprint. Each version description has link to the migration guide.

Following topics addresses these kind of changes.

## Version 2.1.1
1. BentoArchive class has been moved to bentoml folder. Please, consider to change import in your code:
   ```from sinara.bentoml_artifacts import BentoArchive``` to ```from sinara.bentoml.bento_archive import BentoArchive```
2. Added code to support BentoService profiles, in particular ONNX profile. Now Docker image with ONNX model can be created for production.
3. Methods **save_bentoservice** and **load_bentoservice** can not be called in one notebook due to technical realization details.
4. If **step.XXX.py** ended up with the message: ```"Exception while deleting Spark temp dir: /tmp/spark-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX java.nio.file.NoSuchFileException: /tmp/spark-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX```, it is normal behaviour and it can be fixed by running:
```rm -f /tmp/spark-XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX```

## Version 1.1.2
1. Added loading of pipeline params and step params to the dedicated cell in the notebook.
2. Only substep params are defined in the 'parameters' cell and replaced by the job run.
3. Unnecessary default_param_values removed from the 'interface' cell.

[Migration Guide to the Sinara Lib version 1.1.2](https://github.com/4-DS/sinara/blob/main/SINARA_1.1.2_MIGRATION_GUIDE.md)

