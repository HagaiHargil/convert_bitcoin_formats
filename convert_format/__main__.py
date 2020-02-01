import pathlib

import PySimpleGUI as sg

from pipeline import run


def convert_button(fname) -> str:
    result = run(fname)
    return result


sg.theme('Light Blue 2')

conversion_summary = "No files were loaded"
dispatch_dict = {'Convert': convert_button}
layout = [[sg.Text('Choose a file to convert:')],
          [sg.Text('Filename', size=(8, 1)), sg.Input(size=(50, 1)), sg.FileBrowse()],
          [sg.Text('_' * 30)],
          [sg.Text(conversion_summary, key="summary", size=(100, 10), background_color='white', font=('Helvetica', 16))],
          [sg.Button('Convert'), sg.Quit()]]

window = sg.Window("Helbaz's Cointrader Converter ", layout)

while True:
    event, value = window.read()
    if event in ('Quit', None):
        break
    if event in dispatch_dict:
        conversion_summary = dispatch_dict[event](pathlib.Path(value[0]))
        window["summary"].update(conversion_summary)

window.close()
