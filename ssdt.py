import sys
import os
import subprocess
import smartsheet

if sys.version_info[0] < 3 and sys.version_info[1] < 5:
    sys.exit('Error: this script requires Python3.5')


class SsDt:

    def __init__(self, api_key):

        if api_key is None:
            sys.exit('Error: API key is none')

        self.ss = smartsheet.Smartsheet(api_key)
        self.ss.errors_as_exceptions(True)

        self.dt_sheet_id = 5216932677871492
        self.confluence_sheet_id = 3521933800171396

        self.dt_fields = ['Manual Demux']

        self.confluence_fields = ['MGI QC', 'Transfer To GTAC', 'Method of Transfer', 'Analysis/Transfer Instructions',
                                  'Data Recipients', 'Deliverables', 'Pipeline', 'Administration Project',
                                  'Description', 'Billing Account', 'Facilitator', 'Assay',
                                  'Novel Processing Considerations', 'Production Processing Comments']

        # Smartsheet test
        try:
            self.ss.Sheets.get_sheet(self.dt_sheet_id)
        except Exception as e:
            print('Error: failed to connect to DT sheet - {}'.format(e.message))

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

                    if woid and col_ids[cell.column_id] in self.dt_fields:
                        dt_woids[woid][col_ids[cell.column_id]] = cell.value

        return dt_woids

    def get_confluence_woid_data(self, woid_dict: dict) -> dict:
        """:return: dict with confluence woid information {woid: {confluence}}"""

        confluence_col_ids = self.get_column_ids(self.confluence_sheet_id)

        for row in self.ss.Sheets.get_sheet(self.confluence_sheet_id).rows:

            woid = False

            for cell in row.cells:

                if cell.column_id == confluence_col_ids['Work Order ID'] and cell.value in woid_dict:
                    woid = cell.value
                    continue

                if woid and confluence_col_ids[cell.column_id] in self.confluence_fields:
                    woid_dict[woid][confluence_col_ids[cell.column_id]] = cell.value

        return woid_dict

    def run_dt(self, confluence_dict: dict) -> bool:
        # run:
        # /gscuser/acemory/globus_dt_pipeline/stage_and_transfer_dt.py
        return False

    def complete_work_order_in_dt(self):
        # check dt complete for work orders with successful run dt
        pass

    def update_dt_complete_tracking_sheets(self):
        pass

    def update_dt_mss(self):
        # dt complete date/status for ssf?
        # qc status for large scale, no dt date
        pass
