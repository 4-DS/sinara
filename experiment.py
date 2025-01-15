import papermill
import nbformat
import ast

from sinara.substep import get_tmp_work_path


class CustomNodeTransformer(ast.NodeTransformer):

    _assignments = []
    
    def extract_definitions(self, source):
        self._assignments = []
        nodes = ast.parse(source)
        self.visit(nodes)
        return self._assignments
    
    def visit_Assign(self, node):
        #print(node.targets[0].id)
        self._assignments.append(node.targets[0].id)

def first_cell_by_tag(nb, tag):
    tag = tag.lower()
    for cell in nb.cells:
        if cell.cell_type == 'code':
            tags = cell.get('metadata', {}).get('tags', [])
            if any([i.lower() == tag for i in tags]):
                return cell

def extract_parameters(nb):
    params = []
    cell = first_cell_by_tag(nb, 'parameters')
    # if cell is None:
    #     # first code cell
    #     #cell = first_code_cell(nb)
    #     cell = next((c for c in nb.cells if cell.cell_type == 'code'), None)
    if not cell is None:
        params = CustomNodeTransformer().extract_definitions(cell.source)
    return params

def create_function(function_name, function_arguments, function_code):
    function_string = f"def {function_name}({function_arguments}): {function_code}"
    exec(function_string)
    return_value = eval(function_name)
    return return_value

def read_notebook_output(notebook_path):
    with open(notebook_path, 'r', encoding='utf-8') as f:
        notebook_content = nbformat.read(f, as_version=4)
    #print(len(notebook_content.cells))
    result = None
    for cell in reversed(notebook_content.cells):
        output = cell.outputs
        if output and output[0].hasattr("data"):
            if 'text/plain' in output[0].data:
                result = output[0].data['text/plain']
                break
                
    if not result is None:
        result = ast.literal_eval(result)
    return result

class Experiment():

    @staticmethod
    def function(nbname, tag = ""):
        nb = nbformat.read(nbname, as_version=4)
        #print(nbname)
        func_name = nbname.replace('.', '_')
        params_list = extract_parameters(nb)
        func_arguments = ','.join(params_list) #'name, dept'

        parameters = ""
        for p in params_list:
            parameters = parameters + f"'{p}': {p},\n"

        outnbname = nbname.replace('.ipynb', '_' + tag + '.ipynb') if tag else nbname
        outnbname = f"{get_tmp_work_path()}/{outnbname}"
        #print(outnbname)
        func_code = """
        #print(f"Hello, {name}!")
        print(f"Hello from %nbname%!")
        parameters = {
        %parameters%
        }
        #outnbname = f"%out%/%nbname%"
        papermill.execute.execute_notebook("%nbname%", "%outnbname%", parameters=parameters)
        return read_notebook_output("%outnbname%")
        """.replace('%nbname%', nbname).replace('%parameters%', parameters) \
        .replace('%outnbname%', outnbname)
        return create_function(func_name, func_arguments, func_code)
        

        # nn = papermill.execute.execute_notebook(temp_nb_name,
        #                                         commit_report_path,
        #                                         kernel_name=jupyter_kernel_name,
        #                                         # actual pipeline and step parameters are loaded in the notebook
        #                                         parameters={"substep_params": params["substep_params"]},
        #                                         log_output=verbose_output,
        #                                         autosave_cell_every=10,
        #                                         stdout_file=stdout,
        #                                         stderr_file=stderr)