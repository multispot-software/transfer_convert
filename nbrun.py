# Copyright (c) 2015-2017 Antonino Ingargiola
# License: MIT
"""
nbrun - Run an Jupyter/IPython notebook, optionally passing arguments.

USAGE
-----

Copy this file in the folder containing the master notebook used to
execute the other notebooks. Then use `run_notebook()` to execute
notebooks.
"""

import time
from pathlib import Path
from IPython.display import display, FileLink
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

__version__ = '0.2'


def dict_to_code(mapping):
    """Convert input dict `mapping` to a string containing python code.

    Each key is the name of a variable and each value is
    the variable content. Each variable assignment is separated by
    a newline.

    Keys must be strings, and cannot start with a number (i.e. must be
    valid python identifiers). Values must be objects with a string
    representation (the result of repr(obj)) which is valid python code for
    re-creating the object.
    For examples, numbers, strings or list/tuple/dict
    of numbers and strings are allowed.

    Returns:
        A string containing the python code.
    """
    lines = ("{} = {}".format(key, repr(value))
             for key, value in mapping.items())
    return '\n'.join(lines)


def run_notebook(notebook_path, out_notebook_path=None,
                 suffix='-out', nb_kwargs=None, hide_input=False,
                 insert_pos=1, timeout=3600, execute_kwargs=None,
                 display_links=True):
    """Runs a notebook and saves the output in a new notebook.

    Executes a notebook, optionally passing "arguments" in a way roughly
    similar to passing arguments to a function.
    Notebook arguments are passed in a dictionary (`nb_kwargs`) which is
    converted to a string containing python code, then inserted in the notebook
    as a code cell. The code contains only assignments of variables which
    can be used to control the execution of a suitably written notebook. When
    calling a notebook, you need to know which arguments (variables) to pass.
    Differently from functions, no check on the input arguments is performed.
    The "notebook signature" is only informally declared in a conventional
    markdown cell at the beginning of the notebook.

    Arguments:
        notebook_path (pathlib.Path or string): path of the notebook to be
            executed.
        out_notebook_path (pathlib.Path or string or None): complete path
            for the output notebook. If None, saves the notebook in the same
            folder as the template adding a suffix. If not None, `suffix`
            is ignored.
        suffix (string): suffix to append to the file name of the executed
            notebook. Argument ignored if `out_notebook_path` is not None.
        nb_kwargs (dict or None): If not None, this dict is converted to a
            string of python assignments with keys representing variables
            names and values variables content. This string is inserted as
            code-cell in the notebook to be executed.
        insert_pos (int): position of insertion of the code-cell containing
            the input arguments. Default is 1 (i.e. second cell). With this
            default, the input notebook can define, in the first cell, default
            values of input arguments (used when the notebook is executed
            with no arguments or through the Notebook GUI).
        timeout (int): timeout in seconds after which the execution is aborted.
        execute_kwargs (dict): additional arguments passed to
            `ExecutePreprocessor`.
        hide_input (bool): whether to create a notebook with input cells
            hidden (useful to remind user that the auto-generated output
            is not meant to have the code edited.
        display_links (bool): if True, display/print "link" of template and
            output notebooks. Links are only rendered if in a notebook.
            In a text terminal, links are displayed only as file names.
    """
    timestamp_cell = ("**Executed:** %s\n\n**Duration:** %d seconds.\n\n"
                      "**Autogenerated from:** [%s](%s)")
    if nb_kwargs is not None:
        header = '# Cell inserted during automated execution.'
        code = dict_to_code(nb_kwargs)
        code_cell = '\n'.join((header, code))

    notebook_path = Path(notebook_path)
    if not notebook_path.is_file():
        raise FileNotFoundError("Path '%s' not found." % notebook_path)

    if out_notebook_path is None:
            out_notebook_path = Path(notebook_path.parent,
                                     notebook_path.stem + suffix + '.ipynb')
    out_notebook_path = Path(out_notebook_path)
    if not out_notebook_path.parent.exists():
        raise FileNotFoundError("Output path '%s' not found." %
                                out_notebook_path.parent)
    if display_links:
        display(FileLink(str(notebook_path)))

    if execute_kwargs is None:
        execute_kwargs = {}
    ep = ExecutePreprocessor(timeout=timeout, **execute_kwargs)
    nb = nbformat.read(str(notebook_path), as_version=4)

    if hide_input:
        nb["metadata"].update({"hide_input": True})

    if len(nb_kwargs) > 0:
        nb['cells'].insert(insert_pos, nbformat.v4.new_code_cell(code_cell))

    start_time = time.time()
    try:
        # Execute the notebook
        ep.preprocess(nb, {'metadata': {'path': './'}})
    except:
        # Execution failed, print a message then raise.
        msg = ('Error executing the notebook "%s".\n'
               'Notebook arguments: %s\n\n'
               'See notebook "%s" for the traceback.' %
               (notebook_path, str(nb_kwargs), out_notebook_path))
        print(msg)
        raise
    else:
        # On successful execution, add timestamping cell
        duration = time.time() - start_time
        timestamp_cell = timestamp_cell % (time.ctime(start_time), duration,
                                           notebook_path, out_notebook_path)
        nb['cells'].insert(0, nbformat.v4.new_markdown_cell(timestamp_cell))
    finally:
        # Save the notebook even when it raises an error
        nbformat.write(nb, str(out_notebook_path))
        if display_links:
            display(FileLink(str(out_notebook_path)))
