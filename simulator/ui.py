import tkinter as tk
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg)
from matplotlib.figure import Figure
import pandas as pd


def get_data_sample() -> pd.DataFrame:
    data = {'Date': ['2020-06-01', '2020-06-02', '2020-06-03', '2020-06-04',
                     '2020-06-05', '2020-06-06', '2020-06-07', '2020-06-08',
                     '2020-06-09', '2020-06-10'],
            'Close': [9.8, 12, 8, 7.2, 6.9, 7, 6.5, 6.2, 5.5, 6.3]
            }
    df = pd.DataFrame.from_dict(data)
    return df

class Application(tk.Frame):
    def __init__(self, master=None):
        super(Application, self).__init__(master)
        self.master = master
        self.master.title("Trazy Simulator")
        self.grid()

        # Parameters frame
        self.draw_parameter_labelframe(0)
        self.draw_visualization_labelframe(1)

    def draw_parameter_labelframe(self, row):
        lblframe_parameters = tk.LabelFrame(self, text="Parameter")
        lblframe_parameters.grid(row=row, padx=10, pady=5, sticky="ew")
        self.draw_strategy_dropdown(lblframe_parameters, 0)
        self.draw_dates_parameter_label(lblframe_parameters, 1)
        self.draw_action_triggers(lblframe_parameters, 3)

    def draw_visualization_labelframe(self, row):
        lblframe_visualization = tk.LabelFrame(self, text="Visualization")
        lblframe_visualization.grid(row=row, padx=10, pady=5, sticky="ew")
        self.draw_chart_visualization(lblframe_visualization, 0)

    def draw_strategy_dropdown(self, container, row):
        lbl_strategy = tk.Label(container, text="Strategy", width=20, anchor="sw")
        lbl_strategy.grid(row=row, column=0, pady=5, padx=5, sticky="w")
        variable = tk.StringVar(self)
        variable.set("SMA-Crossover")# default value

        dropdown_strategy = tk.OptionMenu(container, variable, "DumbLong", "DumbShort", "SMA-Crossover", "MACD")
        dropdown_strategy.config(width=15)
        dropdown_strategy.grid(row=row, column=1, sticky="ew")

    def draw_dates_parameter_label(self, container, row):
        lbl_start_date = tk.Label(container, text="Start date", width=20, anchor="sw")
        lbl_start_date.grid(row=row, column=0, pady=5, padx=5, sticky="w")
        txt_start_date = tk.Entry(container, width=20)
        txt_start_date.grid(row=row, column=1, columnspan=3, sticky="w")
        txt_start_date.insert(0, "2020-05-01")

        lbl_end_date = tk.Label(container, text="End date", width=20, anchor="sw")
        lbl_end_date.grid(row=row+1, column=0, pady=5, padx=5, sticky="w")
        txt_end_date = tk.Entry(container, width=20)
        txt_end_date.grid(row=row+1, column=1, columnspan=3, sticky="w")
        txt_end_date.insert(0, "2020-06-01")

    def draw_chart_visualization(self, container, row):
        figure = Figure(figsize=(6, 5), dpi=100)
        ax = figure.add_subplot(111)
        df = get_data_sample()
        df.plot(kind='line', legend=True, ax=ax, color='r', marker='o', fontsize=10)
        ax.set_title('Price Movement')
        canvas = FigureCanvasTkAgg(figure, container)
        canvas.draw()
        canvas.get_tk_widget().grid(row=row, column=0, columnspan=3)

    def draw_action_triggers(self, container, row):
        btn_save = tk.Button(container, text="Run", width=15)
        btn_save.grid(row=row, column=0, columnspan=2, padx=5, pady=5, sticky='s')

root = tk.Tk()
app = Application(master=root)
app.mainloop()
