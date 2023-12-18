import csv
from pony.orm import Database, Optional, db_session, sql_debug
import pandas as pd
import logging
from datetime import datetime

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def read_csv(csv_file):

    # read csv file
    file = open(csv_file,'r')
    df = csv.DictReader(file)
    return df

df = pd.read_csv('./data/group.csv')

# df['REMAINING_QUANTITY'].apply(pd.to_numeric).astype(int)


# df=df['REMAINING_QUANTITY'].astype(int)

df.fillna(0, inplace=True)

# data = csv.DictWriter(file, delimiter=',', fieldnames=headers)


# Database Class
db = Database()

# the different configurations
db_params = dict(provider="mysql", host="localhost",
                 user="root", password="root", db="huruma")


# if __name__ == "__main__":
     


class Privileges(db.Entity):
        _table = "Privileges"
        fullName=Optional(str, default='' ,sql_type='TEXT')
        viewWaitingPatient=Optional(str, default='' ,sql_type='TEXT')
        viewPatientsSeenOnSelectedPeriod=Optional(str, default='' ,sql_type='TEXT')
        registerPatient=Optional(str, default='' ,sql_type='TEXT')
        registerInPatient=Optional(str, default='' ,sql_type='TEXT')
        viewPatients=Optional(str, default='' ,sql_type='TEXT')
        viewAdmittedPatients=Optional(str, default='' ,sql_type='TEXT')
        viewAppointments=Optional(str, default='' ,sql_type='TEXT')
        viewPatientCharges=Optional(str, default='' ,sql_type='TEXT')
        reconcilePersonalChargesPayments=Optional(str, default='' ,sql_type='TEXT')
        reconcileCreditPayments=Optional(str, default='' ,sql_type='TEXT')
        dailyCashReports=Optional(str, default='' ,sql_type='TEXT')
        dailyCreditReports=Optional(str, default='' ,sql_type='TEXT')
        viewInvoices=Optional(str, default='' ,sql_type='TEXT')
        purchaseVaccine=Optional(str, default='' ,sql_type='TEXT')
        vaccinePurchasesReport=Optional(str, default='' ,sql_type='TEXT')
        vaccineSalesReport=Optional(str, default='' ,sql_type='TEXT')
        vaccineStockReport=Optional(str, default='' ,sql_type='TEXT')
        recordExpense=Optional(str, default='' ,sql_type='TEXT')
        expensesReport=Optional(str, default='' ,sql_type='TEXT')
        itemsThatNeedReorder=Optional(str, default='' ,sql_type='TEXT')
        registerUser=Optional(str, default='' ,sql_type='TEXT')
        viewUsers=Optional(str, default='' ,sql_type='TEXT')
        registerItems=Optional(str, default='' ,sql_type='TEXT')
        viewItems=Optional(str, default='' ,sql_type='TEXT')
        uploadHospitalLogo=Optional(str, default='' ,sql_type='TEXT')
        logoDetails=Optional(str, default='' ,sql_type='TEXT')
        viewWaitingPatientsForLab=Optional(str, default='' ,sql_type='TEXT')
        viewPatientVisitsOnLab=Optional(str, default='' ,sql_type='TEXT')
        viewExternalPatientVisitsOnLab=Optional(str, default='' ,sql_type='TEXT')
        viewWaitingPatientsForPharmacy=Optional(str, default='' ,sql_type='TEXT')
        purchaseMedicine=Optional(str, default='' ,sql_type='TEXT')
        medicinePurchasesReport=Optional(str, default='' ,sql_type='TEXT')
        medicineSalesReport=Optional(str, default='' ,sql_type='TEXT')
        medicineStockReport=Optional(str, default='' ,sql_type='TEXT')
        viewWaitingPatientsForRadiology=Optional(str, default='' ,sql_type='TEXT')
        viewPatientVisitsOnRadiology=Optional(str, default='' ,sql_type='TEXT')
        viewExternalPatientVisitsOnRadiology=Optional(str, default='' ,sql_type='TEXT')
        editPatientCharges=Optional(str, default='' ,sql_type='TEXT')
        collectionsPerServiceCorporateReport=Optional(str, default='' ,sql_type='TEXT')
        collectionsPerServiceNonCorporateReport=Optional(str, default='' ,sql_type='TEXT')
        cancelTransaction=Optional(str, default='' ,sql_type='TEXT')
        editPersonalChargePayment=Optional(str, default='' ,sql_type='TEXT')
        viewEditedPaymentsReport=Optional(str, default='' ,sql_type='TEXT')
        handlePayroll =Optional(str, default='' ,sql_type='TEXT')
        recordVitalSigns=Optional(str, default='' ,sql_type='TEXT')
        chartOfAccounts=Optional(str, default='' ,sql_type='TEXT')
        generalJournal=Optional(str, default='' ,sql_type='TEXT')
        banking=Optional(str, default='' ,sql_type='TEXT')
        pettyCash=Optional(str, default='' ,sql_type='TEXT')
        taxes=Optional(str, default='' ,sql_type='TEXT')
        generalAccountingItems=Optional(str, default='' ,sql_type='TEXT')
        moneyInItems=Optional(str, default='' ,sql_type='TEXT')
        moneyOutItems=Optional(str, default='' ,sql_type='TEXT') 
        viewAccountingReport=Optional(str, default='' ,sql_type='TEXT')
        medicineStockAdjustment=Optional(str, default='' ,sql_type='TEXT')
        createConsultationRetainerInvoice=Optional(str, default='' ,sql_type='TEXT')
        medicineBatchExpiryReport=Optional(str, default='' ,sql_type='TEXT')
        drugsDispensedReversal=Optional(str, default='' ,sql_type='TEXT')
        removeUndispensedDrugs=Optional(str, default='' ,sql_type='TEXT')
        editInvoicePayment=Optional(str, default='' ,sql_type='TEXT')
        accounting=Optional(str, default='' ,sql_type='TEXT')
        maternity=Optional(str, default='' ,sql_type='TEXT')
        mortuary=Optional(str, default='' ,sql_type='TEXT')
        stores=Optional(str, default='' ,sql_type='TEXT')
        issueItemsToCostCentres=Optional(str, default='' ,sql_type='TEXT')
        loanItemsToCostCentres=Optional(str, default='' ,sql_type='TEXT')
        internalTransfers=Optional(str, default='' ,sql_type='TEXT')
        stockAdjustment=Optional(str, default='' ,sql_type='TEXT')
        inventoryReports=Optional(str, default='' ,sql_type='TEXT')
        viewInpatientAdmissionProfile=Optional(str, default='' ,sql_type='TEXT')
        viewInterimInpatientBill=Optional(str, default='' ,sql_type='TEXT')
        dischargePatient=Optional(str, default='' ,sql_type='TEXT')
        cancelAdmission=Optional(str, default='' ,sql_type='TEXT')
        uploadAppointmentFiles=Optional(str, default='' ,sql_type='TEXT')
        viewPatientsDiagnoses=Optional(str, default='' ,sql_type='TEXT')
        receiptPartialPayments=Optional(str, default='' ,sql_type='TEXT')
        insuranceDrugsSalesAllowanceMapping=Optional(str, default='' ,sql_type='TEXT')
        billPatientMeals=Optional(str, default='' ,sql_type='TEXT')
        removeLabAndRadiologyRequests=Optional(str, default='' ,sql_type='TEXT')
        editPersonalChargeParticulars=Optional(str, default='' ,sql_type='TEXT')
        editInvoiceParticulars=Optional(str, default='' ,sql_type='TEXT')
        addAndModifyBillsOfClosedPatientVisits=Optional(str, default='' ,sql_type='TEXT')
        viewPhysiotherapyStock=Optional(str, default='' ,sql_type='TEXT')
        assets=Optional(str, default='' ,sql_type='TEXT')
        vehiclesAndMileage=Optional(str, default='' ,sql_type='TEXT')
        createAndViewPurchaseOrders=Optional(str, default='' ,sql_type='TEXT')
        removeUndispensedPharmacyRequest=Optional(str, default='' ,sql_type='TEXT')
        addInvoicePayment=Optional(str, default='' ,sql_type='TEXT')
        leaveManagement=Optional(str, default='' ,sql_type='TEXT')
        employeePerformanceManagement=Optional(str, default='' ,sql_type='TEXT')
        editAccountsFromGeneralJournal=Optional(str, default='' ,sql_type='TEXT')
        editMedicinePurchase=Optional(str, default='' ,sql_type='TEXT')
        editPettyCashTransaction=Optional(str, default='' ,sql_type='TEXT')
        viewGeneralLedgerReport=Optional(str, default='' ,sql_type='TEXT')
        viewTrialBalanceReport=Optional(str, default='' ,sql_type='TEXT')
        viewProfitAndLossReport=Optional(str, default='' ,sql_type='TEXT')
        viewDepartmentalIncomeReport=Optional(str, default='' ,sql_type='TEXT')
        viewDepartmentalExpensesReport=Optional(str, default='' ,sql_type='TEXT')
        viewBalanceSheetReport=Optional(str, default='' ,sql_type='TEXT')
        viewPayablesAgingReport=Optional(str, default='' ,sql_type='TEXT')
        viewSupplierBalancesReport=Optional(str, default='' ,sql_type='TEXT')
        viewSupplierStatementReport=Optional(str, default='' ,sql_type='TEXT')
        donations=Optional(str, default='' ,sql_type='TEXT')
        handleVirtualPatients=Optional(str, default='' ,sql_type='TEXT')
        viewMinistryOfHealthReports = Optional(str, default='' ,sql_type='TEXT')
        viewLaboratoryPriceList=Optional(str, default='' ,sql_type='TEXT')
        viewRadiologyPriceList=Optional(str, default='' ,sql_type='TEXT')
        viewMedicinePriceList=Optional(str, default='' ,sql_type='TEXT')
        deleteAppointment=Optional(str, default='' ,sql_type='TEXT')
        viewNhifRebateReport=Optional(str, default='' ,sql_type='TEXT')
        viewInsuranceOutPatientCapitationReport=Optional(str, default='' ,sql_type='TEXT')
        setAppointmentIndividualLimits=Optional(str, default='' ,sql_type='TEXT')
        issuePatientRefunds=Optional(str, default='' ,sql_type='TEXT')
        finalizeInvoices=Optional(str, default='' ,sql_type='TEXT')
        reverseFinalizedInvoices=Optional(str, default='' ,sql_type='TEXT')
        changInpatientAdmissionDate=Optional(str, default='' ,sql_type='TEXT')
        reverseDischarge=Optional(str, default='' ,sql_type='TEXT')
        convertPatientBillsFromCashToCorporateAndViceVersa=Optional(str, default='' ,sql_type='TEXT')
        billInpatientPackages=Optional(str, default='' ,sql_type='TEXT')
        changePaymentAccountOfAdmittedPatients=Optional(str, default='' ,sql_type='TEXT')
        editApprovedLeaveRequest=Optional(str, default='' ,sql_type='TEXT')
        defineInsurancesOpeningBalance=Optional(str, default='' ,sql_type='TEXT')
        defineSuppliersOpeningBalances=Optional(str, default='' ,sql_type='TEXT')
        viewCashFlowStatementReport=Optional(str, default='' ,sql_type='TEXT')
        bankLoans=Optional(str, default='' ,sql_type='TEXT')
        setCopayDetails=Optional(str, default='' ,sql_type='TEXT')
        viewManagementReports=Optional(str, default='' ,sql_type='TEXT')
        handleMainStoreTransactions=Optional(str, default='' ,sql_type='TEXT')
        patientInterbranchTransfer=Optional(str, default='' ,sql_type='TEXT')
        


sql_debug(True)

# # bind the different attributes
db.bind(**db_params)
db.generate_mapping(create_tables=True)  # Create tables


# ================================= Saving to the DB =========================

data =[]
for _, v in df.items():
    data.append(v)


print((df))

@db_session
def save():
    for idx, row in df.iterrows():
        Privileges(
            fullName=str(row["User Full Name"]),
            viewWaitingPatient=str(row["view waiting patients"]),
            viewPatientsSeenOnSelectedPeriod=str(row["view patients seen on a selected period"]),
            registerPatient=str(row["register patient"]),
            registerInPatient=str(row["register inpatient"]),
            viewPatients=str(row["view patients"]),
            viewAdmittedPatients=str(row["view admitted patients"]),
            viewAppointments=str(row["view appointments"]),
            viewPatientCharges=str(row["view patient charges"]),
            reconcilePersonalChargesPayments=str(row["reconcile personal charges payments"]),
            reconcileCreditPayments=str(row["reconcile credit payments"]),
            dailyCashReports=str(row["daily cash reports"]),
            dailyCreditReports=str(row["daily credit reports"]),
            viewInvoices=str(row["view invoices"]),
            purchaseVaccine=str(row["purchase vaccine"]),
            vaccinePurchasesReport=str(row["vaccine purchases report"]),
            vaccineSalesReport=str(row["vaccine sales report"]),
            vaccineStockReport=str(row["vaccine stock report"]),
            recordExpense=str(row["record expense"]),
            expensesReport=str(row["expenses report"]),
            itemsThatNeedReorder=str(row["items that need reorder"]),
            registerUser=str(row["register user"]),
            viewUsers=str(row["view users"]),
            registerItems=str(row["register items"]),
            viewItems=str(row["view items"]),
            uploadHospitalLogo=str(row["upload hospital logo"]),
            logoDetails=str(row["logo details"]),
            viewWaitingPatientsForLab=str(row["view waiting patients for lab"]),
            viewPatientVisitsOnLab=str(row["view patient visits on lab"]),
            viewExternalPatientVisitsOnLab=str(row["view external patient visits on lab"]),
            viewWaitingPatientsForPharmacy=str(row["view waiting patients for pharmacy"]),
            purchaseMedicine=str(row["purchase medicine"]),
            medicinePurchasesReport=str(row["medicine purchases report"]),
            medicineSalesReport=str(row["medicine sales report"]),
            medicineStockReport=str(row["medicine stock report"]),
            viewWaitingPatientsForRadiology=str(row["view waiting patients for radiology"]),
            viewPatientVisitsOnRadiology=str(row["view patient visits on radiology"]),
            viewExternalPatientVisitsOnRadiology=str(row["view external patient visits on radiology"]),
            editPatientCharges=str(row["edit patient charges"]),
            collectionsPerServiceCorporateReport=str(row["collections per service corporate report"]),
            collectionsPerServiceNonCorporateReport=str(row["collections per service non corporate report"]),
            cancelTransaction=str(row["cancel transaction"]),
            editPersonalChargePayment=str(row["edit personal charge payment"]),
            viewEditedPaymentsReport=str(row["view edited payments report"]),
            handlePayroll =str(row ["handle payroll"]),
            recordVitalSigns=str(row["record vital signs"]),
            chartOfAccounts=str(row["chart of accounts"]),
            generalJournal=str(row["general journal"]),
            banking=str(row["banking"]),
            pettyCash=str(row["petty cash"]),
            taxes=str(row["taxes"]),
            generalAccountingItems=str(row["general accounting items"]),
            moneyInItems=str(row["money in items"]),
            moneyOutItems=str(row["money out items"]),
            viewAccountingReport=str(row["view accounting reports"]),
            medicineStockAdjustment=str(row["medicine stock adjustment"]),
            createConsultationRetainerInvoice=str(row["create consultation retainer invoice"]),
            medicineBatchExpiryReport=str(row["medicine batch expiry report"]),
            drugsDispensedReversal=str(row["drugs dispensed reversal"]),
            removeUndispensedDrugs=str(row["remove undispensed drugs"]),
            editInvoicePayment=str(row["edit invoice payment"]),
            accounting=str(row["discounting"]),
            maternity=str(row["maternity"]),
            mortuary=str(row["mortuary"]),
            stores=str(row["stores"]),
            issueItemsToCostCentres=str(row["issue items to cost centres"]),
            loanItemsToCostCentres=str(row["loan items to cost centres"]),
            internalTransfers=str(row["internal transfers"]),
            stockAdjustment=str(row["stock adjustment"]),
            inventoryReports=str(row["inventory reports"]),
            viewInpatientAdmissionProfile=str(row["view inpatient admission profile"]),
            viewInterimInpatientBill=str(row["view interim inpatient bill"]),
            dischargePatient=str(row["discharge patient"]),
            cancelAdmission=str(row["cancel admission"]),
            uploadAppointmentFiles=str(row["upload appointment files"]),
            viewPatientsDiagnoses=str(row["view patients diagnoses"]),
            receiptPartialPayments=str(row["receipt partial payments"]),
            insuranceDrugsSalesAllowanceMapping=str(row["insurance drugs sales allowance mapping"]),
            billPatientMeals=str(row["bill patient meals"]),
            removeLabAndRadiologyRequests=str(row["remove lab and radiology requests"]),
            editPersonalChargeParticulars=str(row["edit personal charge particulars"]),
            editInvoiceParticulars=str(row["edit invoice particulars"]),
            addAndModifyBillsOfClosedPatientVisits=str(row["add and modify bills of closed patient visits"]),
            viewPhysiotherapyStock=str(row["view physiotherapy stock"]),
            assets=str(row["assets"]),
            vehiclesAndMileage=str(row["vehicles and mileage"]),
            createAndViewPurchaseOrders=str(row["create and view purchase orders"]),
            removeUndispensedPharmacyRequest=str(row["remove undispensed pharmacy request"]),
            addInvoicePayment=str(row["add invoice payment"]),
            leaveManagement=str(row["leave management"]),
            employeePerformanceManagement=str(row["employee performance management"]),
            editAccountsFromGeneralJournal=str(row["edit accounts from general journal"]),
            editMedicinePurchase=str(row["edit medicine purchase"]),
            editPettyCashTransaction=str(row["edit petty cash transaction"]),
            viewGeneralLedgerReport=str(row["view general ledger report"]),
            viewTrialBalanceReport=str(row["view trial balance report"]),
            viewProfitAndLossReport=str(row["view profit and loss report"]),
            viewDepartmentalIncomeReport=str(row["view departmental income report"]),
            viewDepartmentalExpensesReport=str(row["view departmental expenses report"]),
            viewBalanceSheetReport=str(row["view balance sheet report"]),
            viewPayablesAgingReport=str(row["view payables aging report"]),
            viewSupplierBalancesReport=str(row["view supplier balances report"]),
            viewSupplierStatementReport=str(row["view supplier statement report"]),
            donations=str(row["donations"]),
            handleVirtualPatients=str(row["handle virtual patients"]),
            viewMinistryOfHealthReports=str(row["view ministry of health reports"]),
            viewLaboratoryPriceList=str(row["view laboratory price list"]),
            viewRadiologyPriceList=str(row["view radiology price list"]),
            viewMedicinePriceList=str(row["view medicine price list"]),
            deleteAppointment=str(row["delete appointment"]),
            viewNhifRebateReport=str(row["view nhif rebate report"]),
            viewInsuranceOutPatientCapitationReport=str(row["view insurance out patient capitation report"]),
            setAppointmentIndividualLimits=str(row["set appointment individual limits"]),
            issuePatientRefunds=str(row["issue patient refunds"]),
            finalizeInvoices=str(row["finalize invoices"]),
            reverseFinalizedInvoices=str(row["reverse finalized invoices"]),
            changInpatientAdmissionDate=str(row["change inpatient admission date"]),
            reverseDischarge=str(row["reverse discharge"]),
            convertPatientBillsFromCashToCorporateAndViceVersa=str(row["convert patient bills from cash to corporate and vice versa"]),
            billInpatientPackages=str(row["bill inpatient packages"]),
            changePaymentAccountOfAdmittedPatients=str(row["change payment account of admitted patients"]),
            editApprovedLeaveRequest=str(row["edit approved leave request"]),
            defineInsurancesOpeningBalance=str(row["define insurances opening balance"]),
            defineSuppliersOpeningBalances=str(row["define suppliers opening balances"]),
            viewCashFlowStatementReport=str(row["view cash flow statement report"]),
            bankLoans=str(row["bank loans"]),
            setCopayDetails=str(row["set copay details"]),
            viewManagementReports=str(row["view management reports"]),
            handleMainStoreTransactions=str(row["handle main store transactions"]),
            patientInterbranchTransfer=str(row["patient interbranch transfer"])

        )

logger.info('Saving to Database...')

save()








    



            
