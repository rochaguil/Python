file = 'file.pdf'
from PyPDF2 import PdfFileWriter, PdfFileReader

pdf_reader = PdfFileReader(open(file,'rb'))
for i in range(pdf_reader .numPages):
    pdf_writer = PdfFileWriter()
    page = pdf_reader.getPage(i)
    page.rotateClockwise(90)    
    pdf_writer.addPage(page)    
    with open('document-page%s.pdf' % i,'wb') as outputStream:
        pdf_writer.write(outputStream)
outputStream.close()
