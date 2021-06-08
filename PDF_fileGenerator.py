import pdfkit
import glob as glob
import os

path_wkhtmltopdf = r"C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe"
config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)


for file in glob.glob(r'E:\Data\ReportsToProcess\*.html'):
    pdfkit.from_file(file, f"E:\\Data\\PDF Reports\\{os.path.basename(file)}.pdf",configuration=config)