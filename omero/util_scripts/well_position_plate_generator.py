#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script converts a Dataset of Images to a Plate, with one image per Well. The script Extract well position from filename using regex.
And automatically assign the image to a plate based on the well position in the name.
This script was created based on the "Dataset_To_Plate.py" script from Will Moore (https://github.com/will-moore).
"""


# @author Riccardo Massei
# <a href="mailto:riccardo.massei@ufz.de">riccardo.massei@ufz.de</a>
# @version 1.0

import re
import omero.scripts as scripts
from collections import defaultdict
from omero.gateway import BlitzGateway
import omero
from omero.rtypes import rint, rstring, robject, unwrap, rlong

def parse_well_position_from_filename(filename):
    """
    Extract well position from filename using regex.
    Expected format is [Letter][Number]_file.tiff (e.g., A2_file.tiff or A02_file.tiff)
    """
    match = re.search(r"([A-Za-z]+)(\d+)", filename)
    if match:
        row, col = match.groups()
        return row.upper(), int(col)
    return None, None

def group_images_by_well_position(images):
    """
    Groups images by their well positions extracted from file names.
    Returns a dict with well positions as keys and lists of images as values.
    """
    grouped_images = defaultdict(list)
    for image in images:
        row_str, col = parse_well_position_from_filename(image.getName())
        if row_str is not None and col is not None:
            well_position = (row_str, col)
            grouped_images[well_position].append(image)
    return grouped_images

def add_images_to_plate(conn, images, plate_id, remove_from=None):
    """
    Add the Images to a Plate at their specific well positions extracted from the file names.
    Allows up to 2 images per well.
    """
    update_service = conn.getUpdateService()

    grouped_images = group_images_by_well_position(images)

    for (row_str, col), grouped_images in grouped_images.items():
        row = ord(row_str) - ord('A')  # Convert row letter to 0-based index (e.g., A -> 0, B -> 1)
        col -= 1  # Adjust column to 0-based index

        well = omero.model.WellI()
        well.plate = omero.model.PlateI(plate_id, False)
        well.column = rint(col)
        well.row = rint(row)

        for image in grouped_images[:2]:  # Limit to 2 images per well
            ws = omero.model.WellSampleI()
            ws.image = omero.model.ImageI(image.id, False)
            ws.well = well
            well.addWellSample(ws)

        try:
            update_service.saveObject(well)
        except Exception as e:
            print("Failed to add image(s) to well:", e)
            return False

    # Optionally remove images from Dataset
    if remove_from is not None:
        for image in images:
            links = list(image.getParentLinks(remove_from.id))
            link_ids = [link.id for link in links]
            conn.deleteObjects('DatasetImageLink', link_ids)
    return True

def dataset_to_plate(conn, script_params, dataset_id, screen):
    dataset = conn.getObject("Dataset", dataset_id)
    if dataset is None:
        return None, "Dataset not found", None

    update_service = conn.getUpdateService()

    # Create Plate
    plate = omero.model.PlateI()
    plate.name = rstring(dataset.getName())
    plate = update_service.saveAndReturnObject(plate)

    # Optionally link Plate to Screen
    link = None
    if screen is not None and screen.canLink():
        link = omero.model.ScreenPlateLinkI()
        link.parent = omero.model.ScreenI(screen.id, False)
        link.child = plate
        update_service.saveObject(link)

    # Sort images by name and add them to the plate
    images = list(dataset.listChildren())
    add_images_to_plate(conn, images, plate.getId().getValue(), dataset if script_params.get("Remove_From_Dataset", False) else None)

    return plate, "Images added to plate based on filename", link

def datasets_to_plates(conn, script_params):
    """
    Processes multiple datasets and converts each into a plate based on the provided script parameters.
    Images in each dataset are arranged into wells on the plate according to their parsed well positions from file names.
    """
    # Retrieve dataset IDs from script parameters
    dataset_ids = script_params['IDs']
    screen = None

    if 'Screen' in script_params and script_params['Screen']:
        screen_name_or_id = script_params['Screen']
        try:
            screen_id = int(screen_name_or_id)
            screen = conn.getObject("Screen", screen_id)
        except ValueError:
            # If not an ID, assume it's a new screen name
            new_screen = omero.model.ScreenI()
            new_screen.name = rstring(screen_name_or_id)
            screen = conn.getUpdateService().saveAndReturnObject(new_screen)
            screen = conn.getObject("Screen", screen.id.val)

    # Process each dataset
    plates = []
    for dataset_id in dataset_ids:
        plate, message, link = dataset_to_plate(conn, script_params, dataset_id, screen)
        if plate:
            plates.append(plate)
            print(f"Successfully processed dataset {dataset_id}: {message}")
        else:
            print(f"Failed to process dataset {dataset_id}: {message}")

    # Check if plates were created and provide appropriate feedback
    if not plates:
        return None, "No plates were created."
    else:
        return plates[0], f"{len(plates)} plates were created successfully."

def run_script():
    """
    The main entry point of the script, as called by the client via the
    scripting service, passing the required parameters.
    """

    data_types = [rstring('Dataset')]
    first_axis = [rstring('column'), rstring('row')]
    row_col_naming = [rstring('letter'), rstring('number')]

    client = scripts.client(
        'Well_Position_Plate_Generator.py',
        """Take a Dataset of Images and put them in a new Plate, \
arranging them into rows or columns as desired.
Optionally add the Plate to a new or existing Screen.
Furthermore, the script Extract well position from filename using regex. \
And automatically assign the image to a plate based on the well position in the name.""",

        scripts.String(
            "Data_Type", optional=False, grouping="1",
            description="Choose source of images (only Dataset supported)",
            values=data_types, default="Dataset"),

        scripts.List(
            "IDs", optional=False, grouping="2",
            description="List of Dataset IDs to convert to new"
            " Plates.").ofType(rlong(0)),

        scripts.String(
            "Filter_Names", grouping="2.1",
            description="Filter the images by names that contain this value"),

        scripts.String(
            "First_Axis", grouping="3", optional=False, default='column',
            values=first_axis,
            description="""Arrange images accross 'column' first or down"
            " 'row'"""),

        scripts.Int(
            "First_Axis_Count", grouping="3.1", optional=False, default=12,
            description="Number of Rows or Columns in the 'First Axis'",
            min=1),

        scripts.Int(
            "Images_Per_Well", grouping="3.2", optional=False, default=1,
            description="Number of Images (Well Samples) per Well",
            min=1),

        scripts.String(
            "Column_Names", grouping="4", optional=False, default='number',
            values=row_col_naming,
            description="""Name plate columns with 'number' or 'letter'"""),

        scripts.String(
            "Row_Names", grouping="5", optional=False, default='letter',
            values=row_col_naming,
            description="""Name plate rows with 'number' or 'letter'"""),

        scripts.String(
            "Screen", grouping="6",
            description="Option: put Plate(s) in a Screen. Enter Name of new"
            " screen or ID of existing screen"""),

        scripts.Bool(
            "Remove_From_Dataset", grouping="7", default=True,
            description="Remove Images from Dataset as they are added to"
            " Plate"),

        version="4.3.2",
        authors=["William Moore", "OME Team"],
        institutions=["University of Dundee"],
        contact="ome-users@lists.openmicroscopy.org.uk",
    )

    try:
        script_params = client.getInputs(unwrap=True)

        # wrap client to use the Blitz Gateway
        conn = BlitzGateway(client_obj=client)

        # convert Dataset(s) to Plate(s). Returns new plates or screen
        new_obj, message = datasets_to_plates(conn, script_params)

        client.setOutput("Message", rstring(message))
        if new_obj:
            client.setOutput("New_Object", robject(new_obj))

    finally:
        client.closeSession()


if __name__ == "__main__":
    run_script()