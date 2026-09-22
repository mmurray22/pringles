import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator
import logging
import os

class PktgenGraphs:

    def initialize(self, executable, graph_dir="pktgen_graphs"):
        sns.set_theme()
        self.graph_dir = graph_dir
        
        # Get the directory where the running script/executable is located
        executable_dir = os.path.dirname(os.path.abspath(executable))
        
        # Define the new folder name
        graph_path = os.path.join(executable_dir, self.graph_dir)
        
        # Create the directory safely
        os.makedirs(graph_path, exist_ok=True)

    def write_graph_to_file(self, graph_filename, fig):
        graph_filename = self.graph_dir + "/" + graph_filename
        fig.savefig(graph_filename)
        print(f"Your graph is at {graph_filename}")

    #def create_graph_figure(self, graph_filename, column_y_arr, column_x_arr, df_arr, is_timeseries):

    # Generate single plots
    def graph(self, df, column_y, column_x, title, graph_filename, graph_type):
        f = plt.figure()
        sp = f.add_subplot()
        if graph_type == "line":
            graph = sns.lineplot(x=column_x, y=column_y, data=df)
        elif graph_type == "bar":
            graph = sns.barplot(x=column_x, y=column_y, data=df)
 
        sp.set_title(title, size=12, zorder=0)
        sp.set_xlabel(column_x, fontsize = 12, labelpad = 7)
        sp.set_ylabel(column_y, fontsize = 12)
        f.tight_layout()
        self.write_graph_to_file(graph_filename, f)

    def graph_grid(self, df_array, column_y, column_x, title, graph_filename, graph_type, y, x):
        f = plt.figure(figsize=(14, 14))
        gs = f.add_gridspec(x, y)
        if len(df_array) > (x*y):
            print(f"==================Grid space is too small! Number of dfs: {len(df_array)} but the grid is {x*y}")
            return
        j = 0
        for df in df_array:
            sp = f.add_subplot(gs[j, 0])
            j += 1
            if graph_type == "line":
                graph = sns.lineplot(x=column_x, y=column_y, data=df, ax=sp)
            elif graph_type == "bar":
                graph = sns.barplot(x=column_x, y=column_y, data=df, ax=sp)
            sp.xaxis.set_major_locator(MultipleLocator(1))
 
        #sp.set_title(title, size=12, zorder=0)
        #sp.set_xlabel(column_x, fontsize = 12, labelpad = 7)
        #sp.set_ylabel(column_y, fontsize = 12)
        f.tight_layout()
        self.write_graph_to_file(graph_filename, f)
