import tableauserverclient as TSC
import getpass # For securely prompting password
import requests.exceptions # Import the exceptions from the requests library

def test_tableau_server_access():
    """
    Prompts the user for Tableau Server credentials and attempts to sign in
    and list projects/workbooks as a test of access.
    """
    print("--- Tableau Server Access Test ---")

    # Get server details from user
    tableau_server_url = input("Enter Tableau Server URL (e.g., https://yourserver.com or https://online.tableau.com): ").strip()
    tableau_username = input("Enter Tableau Username: ").strip()
    tableau_password = getpass.getpass("Enter Tableau Password: ").strip() # Secure input
    tableau_site_id = input("Enter Tableau Site ID (leave blank for Default site): ").strip()

    # If the site ID is empty, set it to None for the default site
    if not tableau_site_id:
        tableau_site_id = '' # TSC expects an empty string for the default site

    print("\nAttempting to connect to Tableau Server...")

    try:
        # 1. Initialize TableauAuth object
        # The site_id parameter is for the contentUrl of the site
        tableau_auth = TSC.TableauAuth(
            username=tableau_username,
            password=tableau_password,
            site_id=tableau_site_id
        )

        # 2. Initialize Server object
        # use_server_version=True is highly recommended for compatibility
        server = TSC.Server(tableau_server_url, use_server_version=True)

        # 3. Sign in using a 'with' block for automatic sign-out
        with server.auth.sign_in(tableau_auth):
            print("\n-------------------------------------")
            print("  Successfully signed in to Tableau Server!")
            print(f"  Server Address: {server.server_address}")
            print(f"  API Version: {server.version}")
            print(f"  Signed in to Site Name: '{server.site_name}' (ID: {server.site_id})")
            print("-------------------------------------\n")

            # --- Test API Calls ---

            # Test 1: List Projects
            try:
                print("Retrieving projects...")
                all_projects, _ = server.projects.get() # _ catches pagination_item
                if all_projects:
                    print(f"Found {len(all_projects)} projects on site '{server.site_name}':")
                    for i, project in enumerate(all_projects):
                        print(f"  - [{i+1}] Name: {project.name}, ID: {project.id}")
                        if i >= 9: # List first 10 projects
                            print("  (Displaying only first 10 projects...)")
                            break
                else:
                    print("No projects found on this site.")
            except TSC.ServerError as e:
                print(f"Error retrieving projects: {e}")
            except Exception as e:
                print(f"An unexpected error occurred while listing projects: {e}")


            # Test 2: List Workbooks
            try:
                print("\nRetrieving workbooks...")
                # Using Pager to handle potentially large number of workbooks
                workbooks_count = 0
                for i, workbook in enumerate(TSC.Pager(server.workbooks.get())):
                    print(f"  - [{i+1}] Name: {workbook.name}, Project: {workbook.project_name}, ID: {workbook.id}")
                    workbooks_count += 1
                    if workbooks_count >= 10: # Limit for testing
                        print("  (Displaying only first 10 workbooks...)")
                        break
                if workbooks_count == 0:
                    print("No workbooks found on this site or could not retrieve any.")
            except TSC.ServerError as e:
                print(f"Error retrieving workbooks: {e}")
            except Exception as e:
                print(f"An unexpected error occurred while listing workbooks: {e}")

        print("\nSuccessfully signed out from Tableau Server.")

    except requests.exceptions.ConnectionError as e:
        print(f"\nConnection Error: Could not connect to Tableau Server at '{tableau_server_url}'.")
        print(f"Please check the URL, your network connection, or proxy settings. Error details: {e}")
    except TSC.MissingRequiredFieldError as e:
        print(f"\nConfiguration Error: {e}. Please ensure all required fields are provided (URL, Username, Password).")
    except TSC.EndpointUnavailableError as e:
        print(f"\nAPI Endpoint Unavailable: {e}. The server might be reachable but the API endpoint is not responsive or your server version is too old/new for the current TSC library version.")
    except TSC.UnauthenticatedError:
        print("\nAuthentication Failed: Invalid username, password, or site ID. Please check your credentials.")
    except TSC.ServerError as e:
        print(f"\nTableau Server API Error: {e}. This is a general error from the Tableau Server itself (e.g., permission denied for an action).")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {type(e).__name__}: {e}")

    print("\n--- Test Finished ---")

if __name__ == "__main__":
    test_tableau_server_access()
    
