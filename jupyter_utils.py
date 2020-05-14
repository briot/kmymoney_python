"""
Some utility functions for Jupyter notebooks
"""

from IPython.display import HTML


def disp(df, height=300):
    """
    Display a pivot table so that the header rows and columns remain visible
    on the screen when scrolling horizontally or vertically.

    See also https://github.com/quantopian/qgrid
    simple code copied from  https://github.com/pandas-dev/pandas/issues/29072
    """

    style = f"""
<style scoped>
    .dataframe-div {{
      overflow: auto;
      position: relative;
    }}
    .dataframe thead {{
      position: -webkit-sticky; /* for Safari */
      position: sticky;
      top: 0;
      background: #eee;
      color: black;
    }}
    .dataframe thead th:first-child {{
      left: 0;
      z-index: 1;
    }}
    .dataframe tbody tr th {{
      position: -webkit-sticky; /* for Safari */
      position: sticky;
      left: 0;
      background: #eee;
      color: black;
      vertical-align: top;
    }}
</style>
"""
    display(HTML(
        style +
        f'<div class="dataframe-div" style="max-height:{height}px">'
        + df.to_html()
        + "\n</div>"
    ))


def as_numeric(df):
    """
    When a dataframe contains only numeric values, format them
    """
    return (
        df
        .dropna()           # remove fully empty lines
        .applymap('{:,.2f}'.format)   # format floats with two digits
        .replace('nan', '-')         # replace NaN with '-'
    )
