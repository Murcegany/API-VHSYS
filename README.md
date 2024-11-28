# API - VHSYS - Integration and Automation

This repository provides an example of integration and automation between the **API VHSYS** and a **SQL Server** database. The project, developed in **C#**, demonstrates how to consume data from the API and store it in a structured way in simulated database tables. The solution includes `.exe` files that can be set to run automatically through Windows Task Scheduler.

## Main Features
- **Integration with the VHSYS API**:
  - Use of the API to extract data related to various tables (invoices, customers, accounts receivable, etc.).
  - Respect the limitations imposed by the API.
- **Process Automation**:
  - Automated execution of scripts through `.exe` files.
  - Possibility of scheduling for periodic execution.
- **Data Simulation**:
  - Fictitious data and simulated tables to illustrate how it works.
- **Documentation**:
  - Each repository folder includes specific documentation about the codes present.
- **Task Scheduler Configuration**:
  - Step-by-step guidance for setting up automated execution on Windows.

---

## Repository Structure
- **`.exe`** files:
  - Available in folders for executing automations.
- **Documentation**:
  - Explanatory files in each folder detail the functionalities of the codes.
- **Simulated Tables**:
  - Invoices
  - Customers
  - Accounts Receivable
  - Goods Entry
  - Consumer Notes
  - Products
  - Orders
  - Order Products

---

## Settings for Task Scheduler

### How to Create a Scheduled Task:
1. **Open Task Scheduler in Windows**:
   - Search for *Task Scheduler* in the start menu.

2. **Click on "Create Task"**:
   - Give a name and description for the task.
   - Check the "Run with higher privileges" option if necessary.

3. **Set the Trigger**:
   - Go to the **Triggers** tab and click **New**.
   - Choose the frequency (daily, weekly, etc.) and configure the start time.

4. **Configure the Action**:
   - On the **Actions** tab, click **New**.
   - Choose "Start a program" and select the corresponding `.exe` file.

5. **Configure Additional Settings**:
   - **Conditions**:
     - Configure to run only under specific conditions (for example, when the computer is idle).
   - **Settings**:
     - Allows the execution of the task in specific situations.
     - Define actions for when the task fails.

---

## Observations
- **Fictitious Data**:
  - This repository uses simulated information for demonstration purposes.
- **Simulated Tables**:
  - The database contains representative tables, such as invoices and accounts receivable.
- **Complete Documentation**:
  - Each folder includes technical details about the files and their implementations.

---

## External Resources
- [VHSYS API Documentation](https://developers.vhsys.com.br/api/): Detailed information about available endpoints and features.

---

This repository is a practical solution for those who want to automate integrations with the VHSYS API, offering clear and easy-to-implement examples.