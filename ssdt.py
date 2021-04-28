#!/usr/bin/python3

import sys
import os
import requests
import smartsheet

from collections import namedtuple
from datetime import datetime

from dt import ConfluenceDTAttributes, DTDTAttributes

class Error(Exception):
    """This allows for custom messages when raising exceptions"""


class SsDt:
    """
    dt_fields, confluence_fields
        -both are list of strings that act as dictionary keys and SS column IDs
    """

    # def __init__(self, dt_fields, confluence_fields):
    def __init__(self):
        self.api_key = "uqm83gasjaa79dm2l7dfpau55o"
        self.ss = smartsheet.Smartsheet("uqm83gasjaa79dm2l7dfpau55o")
        self.ss.errors_as_exceptions(True)
        self.dt_sheet_id = 5216932677871492
        self.confluence_sheet_id = 3521933800171396
        self.active_projects_folder_id = 3274710231345028
        # going to pass these
        # self.dt_fields = confluence_fields
        # self.confluence_fields = confluence_fields
        self.dt_fields = DTDTAttributes().values
        self.confluence_fields = ConfluenceDTAttributes().values

        self.date = datetime.now().isoformat()

        should_check_env = False
        self.check_env(should_check_env)

    def remascii(self, s: str) -> str:
        return "".join(i for i in s if ord(i) < 128)

    def check_env(self, should_check_env):
        if sys.version_info[0] < 3 or sys.version_info[1] < 5:
            s = 'Error: this script requires Python3.5'
            raise Error(s)

        # Smartsheet test
        try:
            self.ss.Sheets.get_sheet(self.dt_sheet_id)
        except Exception as e:
            s = 'Error: failed to connect to DT sheet - {}'.format(e.message)
            raise Error(s)

    def get_column_ids(self, sheet_id: int) -> dict:
        """:return: dict of {column_title: column_id} and {column_id: column_title}"""

        c_ids = {}
        for c in self.ss.Sheets.get_columns(sheet_id).data:
            c_ids[c.title] = c.id
            c_ids[c.id] = c.title

        return c_ids

    def get_dt_transfer_wo(self) -> dict:
        """:return: dict of woid's for Data Transfer from DT sheet."""

        dt_woids = dict()
        col_ids = self.get_column_ids(self.dt_sheet_id)

        for row in self.ss.Sheets.get_sheet(self.dt_sheet_id).rows:

            woid = False

            if row.parent_id is None:

                for cell in row.cells:

                    if cell.column_id == col_ids['Work Order ID']:
                        woid = str(cell.value)
                        if '.' in woid:
                            woid = woid.split('.')[0]
                        dt_woids[woid] = dict()
                        continue

                    if woid:

                        if cell.column_id == col_ids['Data Transfer Stage'] and cell.value == 'QC@MGI Complete':

                            attachment_names = []

                            for atch in self.ss.Attachments.list_row_attachments(self.dt_sheet_id, row.id).data:

                                try:
                                    #
                                    url_res = requests.get(self.ss.Attachments.get_attachment(self.dt_sheet_id,
                                                                                              atch.id).url, atch.name)
                                except requests.exceptions.RequestException as e:
                                    s = 'Error: {} attachment failed to download\n'.format(atch.name, e)
                                    raise Error(s)

                                if url_res:
                                    with open(atch.name, 'wb') as f:
                                        f.write(url_res.content)

                                attachment_dir = "/gscmnt/gc5000/download/DT-ATTACHMENTS/"
                                attach_name = os.path.join(attachment_dir, atch.name)
                                attachment_names.append(attach_name)

                            dt_woids[woid][col_ids[cell.column_id]] = cell.value
                            dt_woids[woid]['qc_files'] = attachment_names

                            continue

                        if col_ids[cell.column_id] in self.dt_fields:
                            dt_woids[woid][col_ids[cell.column_id]] = cell.value
        # TESTING
        # print("len(dt_woids): %s" % str(len(dt_woids)))

        return dt_woids

    def get_confluence_woid_data(self, woid_dict: dict) -> dict:
        """:return: dict with confluence woid information {woid: {confluence}}"""

        confluence_col_ids = self.get_column_ids(self.confluence_sheet_id)

        for row in self.ss.Sheets.get_sheet(self.confluence_sheet_id).rows:

            woid = False

            for cell in row.cells:

                if cell.column_id == confluence_col_ids['Work Order ID'] and cell.value in woid_dict:
                    # print("woid: %s" % str(cell.value))
                    # print("type: %s" % str(type(cell.value)))

                    woid = cell.value
                    continue

                if woid:

                    if confluence_col_ids[cell.column_id] in self.confluence_fields:
                        woid_dict[woid][confluence_col_ids[cell.column_id]] = cell.value

                    if cell.column_id == confluence_col_ids['Production Processing Comments']:
                        if cell.value:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = self.remascii(cell.value)
                        else:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = ""
                        continue

                    if cell.column_id == confluence_col_ids['Analysis/Transfer Instructions']:
                        if cell.value:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = self.remascii(cell.value)
                        else:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = ""
                        continue

                    if cell.column_id == confluence_col_ids['Sequencing Recipe']:
                        if cell.value:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = self.remascii(cell.value)
                        else:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = ""
                        continue

                    if cell.column_id == confluence_col_ids['Kit Version']:
                        if cell.value:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = self.remascii(cell.value)
                        else:
                            woid_dict[woid][confluence_col_ids[cell.column_id]] = ""
                        continue

                    # moving to the top of this block as I think having it down here defeats the purpose of the above resmasciis
                    # if confluence_col_ids[cell.column_id] in self.confluence_fields:
                    #     woid_dict[woid][confluence_col_ids[cell.column_id]] = cell.value

        # TESTING
        # print("len(woid_dict): %s" % str(len(woid_dict)))

        return woid_dict

    def complete_wo_dt_con(self, woid: str, dt_pass: bool) -> tuple:
        """
        :return: namedtuple with  woid and dt/con row completed status True/False.
        """

        Results = namedtuple('Results', ['woid', 'dt', 'confluence', 'admin'])
        dt_result = False
        con_result = False
        admin = None

        # Complete work order in DT
        dt_col_ids = self.get_column_ids(self.dt_sheet_id)
        dt_row_found = False
        dt_update = False
        dt_row_updated = False

        for row in self.ss.Sheets.get_sheet(self.dt_sheet_id).rows:

            if not dt_row_updated:

                for cell in row.cells:

                    if cell.column_id == dt_col_ids['Work Order ID']:
                        w = str(cell.value)
                        if '.' in w:
                            w = w.split('.')[0]
                        if w == woid:
                            dt_row_found = True

                    if dt_row_found:

                        updated_row = self.ss.models.Row()
                        updated_row.id = row.id

                        if cell.column_id == dt_col_ids['Move to CDT (completed DT)'] and dt_pass:
                            updated_row.cells.append({'column_id': cell.column_id, 'value': True})
                            updated_row.cells.append({'column_id': dt_col_ids['Data Transfer Completed Date'],
                                                      'value': self.date})
                            dt_update = True

                        if cell.column_id == dt_col_ids['Data Transfer Stage'] and not dt_pass:
                            updated_row.cells.append({'column_id': cell.column_id, 'object_value': 'Failed auto-DT'})
                            dt_update = True

                        if dt_update:
                            resp = self.ss.Sheets.update_rows(self.dt_sheet_id, [updated_row])
                            if resp.message == 'SUCCESS':
                                dt_result = True
                            dt_row_updated = True

        # Complete work order in Confluence
        con_col_ids = self.get_column_ids(self.confluence_sheet_id)
        con_row_found = False
        con_update = False
        con_row_updated = False

        for row in self.ss.Sheets.get_sheet(self.confluence_sheet_id).rows:

            updated_row = self.ss.models.Row()
            updated_row.id = row.id

            for cell in row.cells:

                if not con_row_updated:

                    if cell.column_id == con_col_ids['Work Order ID']:

                        w = str(cell.value)

                        if '.' in w:
                            w = w.split('.')[0]

                        if w == woid:

                            if dt_pass:
                                updated_row.cells.append({'column_id': con_col_ids['Work Order Complete'],
                                                          'value': True})
                                updated_row.cells.append({'column_id': con_col_ids['Data Transfer Complete'],
                                                          'value': self.date})

                            if not dt_pass:
                                updated_row.cells.append({'column_id': con_col_ids['Data Transfer Information'],
                                                          'object_value': 'Human Needed'})
                            con_row_found = True

                    if cell.column_id == con_col_ids['Administration Project'] and con_row_found:
                        admin = cell.value[:50]
                        con_update = True

                    if con_update:
                        resp = self.ss.Sheets.update_rows(self.confluence_sheet_id, [updated_row])
                        if resp.message == 'SUCCESS':
                            con_result = True
                        con_row_updated = True

        return Results(woid, dt_result, con_result, admin)

    def update_dt_mss(self, result_tuple: tuple) -> bool:
        """
        :return: Update MSS sheet row status 'Data Trasnfer Completed' add DT date, True if SUCCESS, False if not.
        """

        if result_tuple.admin is not None:

            for folder in self.ss.Folders.get_folder(self.active_projects_folder_id).folders:

                if folder.name == result_tuple.admin:

                    for sheet in self.ss.Folders.get_folder(folder.id).sheets:

                        sheet_col_ids = self.get_column_ids(sheet.id)
                        updated_rows = []

                        for row in self.ss.Sheets.get_sheet(sheet.id).rows:

                            woid_found = False

                            for cell in row.cells:

                                if not woid_found:

                                    if cell.column_id == sheet_col_ids['Work Order ID'] \
                                            and cell.value == result_tuple.woid:
                                        updated_row = self.ss.models.Row()
                                        updated_row.id = row.id
                                        updated_row.cells.append(
                                            {'column_id': sheet_col_ids['Current Production Status'],
                                             'value': 'Data Transfer Completed'})
                                        updated_row.cells.append(
                                            {'column_id': sheet_col_ids['Data Transfer Completed Date'],
                                             'value': self.date})

                                        updated_rows.append(updated_row)
                                        woid_found = True

                        if len(updated_rows) > 0:
                            resp = self.ss.Sheets.update_rows(sheet.id, updated_rows)
                            if resp.message == 'SUCCESS':
                                return True
                            else:
                                return False
        return False


