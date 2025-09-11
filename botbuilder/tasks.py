from robocorp.tasks import task
from robocorp import browser

from RPA.HTTP import HTTP
from RPA.Tables import Tables
from RPA.PDF import PDF
from RPA.Archive import Archive

from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from PIL import Image

import io
import os

@task
def order_bots_from_rsb():
    """
    Orders robots from RobotSpareBin Industries Inc.
    Saves the order HTML receipt as a PDF file.
    Saves the screenshot of the ordered robot.
    Embeds the screenshot of the robot to the PDF receipt.
    Creates ZIP archive of the receipts and the images.
    """

    browser.configure(
        slowmo=300
    )

    # grab order csv and launch the order form site
    orders = get_orders()
    open_order_website()

    # iterate through the csv to create orders
    for order in orders:
        close_annoying_modal()  # modal seems to appear at the start of each form entry
        complete_form(order)

    # archive all of the orders to a .zip
    archive_receipts()

def open_order_website():
    """Go to the order site"""
    browser.goto("https://robotsparebinindustries.com/#/robot-order")

def get_orders():
    """Downloads and opens the orders csv from the site"""
    http = HTTP()
    http.download("https://robotsparebinindustries.com/orders.csv", overwrite=True)

    table = Tables()
    orders = table.read_table_from_csv("orders.csv",header=True)

    return orders

def close_annoying_modal():
    """Close the popup window at website launch"""

    page = browser.page()
    page.click("button:text('Yep')")

def complete_form(order):
    """Fills out the form using the order object, then calls the export function
    to save the files (receipt pdf and screenshot image)"""

    page = browser.page()
    table = Tables()

    page.select_option("#head", str(order["Head"]))
    
    body_str = "#id-body-"+str(order["Body"])
    page.click(body_str)
    
    page.fill("div.mb-3 input[type=number]", str(order["Legs"]))
    
    page.fill("#address",order["Address"])

    page.click("button:text('Preview')")

    # account for possible errors on order submit
    proceed = False
    while proceed == False:
        page.click("button:text('Order')")
        order_error = page.locator("div.alert-danger")
        if (order_error.count() > 0 ):
            print(f"Retrying order...")
        else:
            proceed = True

    export_as_pdf(str(order["Order number"]))

    page.click("button:text('Order another robot')")

def export_as_pdf(order_num):
    """Given an order number, generates a screenshot from just the receipt <div> section, 
    and saves as a pdf. Takes a screenshot of the preview image and saves as a .png
    Then embeds the preview image into the existing receipt pdf"""

    # create the paths first so we don't get errors while saving
    folder_path_img = "output/img"
    folder_path_receipts = "output/receipts"
    folder_path_orders = "output/orders"

    # create the subfolder if it doesn't exist
    os.makedirs(folder_path_img, exist_ok=True)
    os.makedirs(folder_path_receipts, exist_ok=True)
    os.makedirs(folder_path_orders, exist_ok=True)
    
    page = browser.page()
    element = page.locator("#receipt")

    buffer = element.screenshot(path=None)   # store in memory only - don't need a separate receipt img file

    img = Image.open(io.BytesIO(buffer))
    img_width, img_height = img.size

    # wrap in an ImageReader for output
    img_reader = ImageReader(io.BytesIO(buffer))

    # save into pdf output
    pdf_path = folder_path_receipts + "/robot_order" + order_num + ".pdf"
    c = canvas.Canvas(pdf_path, pagesize=(img_width, img_height))
    c.drawImage(img_reader, 0, 0, width=img_width, height=img_height)
    c.save()

    # capture the element's screenshot
    ss_path = folder_path_img + "/robot_order" + str(order_num) + ".png"
    element = page.locator("#robot-preview-image")
    screenshot = element.screenshot(path=ss_path)

    embed_screenshot_to_receipt(ss_path, pdf_path)

def embed_screenshot_to_receipt(screenshot, pdf_file):
    """Given two paths (screenshot file and pdf file) will add the screenshot
    to the pdf file"""

    pdf = PDF()
    file_list = [
        pdf_file,
        screenshot
    ]
    final_pdf = pdf_file.replace("receipts","orders")
    pdf.add_files_to_pdf(file_list, final_pdf)

def archive_receipts():
    """Creates a .zip of the folder contents for output/orders"""

    archive = Archive()
    archive.archive_folder_with_zip("output/orders", "output/receipts_archive.zip")