# kmymoney_python
Python interface to KMyMoney's SQL files

See the Jupyter Notebook kmymoney.ipynb for examples of the
various queries supported.

Installing
===========

python3 -m venv env
source env/bin/activate
pip install -r requirements.txt 
jupyter labextension install @jupyter-widgets/jupyterlab-manager
jupyter nbextension enable --py --sys-prefix qgrid
