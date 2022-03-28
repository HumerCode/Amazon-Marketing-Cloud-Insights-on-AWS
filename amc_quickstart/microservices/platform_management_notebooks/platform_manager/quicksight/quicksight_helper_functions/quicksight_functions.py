import boto3
import awswrangler as wr
client = boto3.client('quicksight')
client_lf = boto3.client('lakeformation')

class QSHelperFunctions():
    # QS PERMISSION FUNCTIONS
    # ----------------------------

    def __get_permissions_type_qs(self, typ):
        if(typ.upper() == "TEMPLATE"):
            perms = ["quicksight:UpdateTemplatePermissions", 
                      "quicksight:DescribeTemplatePermissions", 
                      "quicksight:UpdateTemplateAlias", 
                      "quicksight:DeleteTemplateAlias", 
                      "quicksight:DescribeTemplateAlias", 
                      "quicksight:ListTemplateAliases", 
                      "quicksight:ListTemplates", 
                      "quicksight:CreateTemplateAlias", 
                      "quicksight:DeleteTemplate", 
                      "quicksight:UpdateTemplate", 
                      "quicksight:ListTemplateVersions", 
                      "quicksight:DescribeTemplate", 
                      "quicksight:CreateTemplate"]

        elif(typ.upper() == "DASHBOARD" or typ.upper() == "DASHBOARD_ADMIN"):
            perms = ["quicksight:DescribeDashboard", 
                        "quicksight:ListDashboardVersions", 
                        "quicksight:UpdateDashboardPermissions", 
                        "quicksight:QueryDashboard", 
                        "quicksight:UpdateDashboard", 
                        "quicksight:DeleteDashboard", 
                        "quicksight:UpdateDashboardPublishedVersion", 
                        "quicksight:DescribeDashboardPermissions"] 
        elif(typ.upper() == "DASHBOARD_USER"):
            perms = ["quicksight:DescribeDashboard", 
                            "quicksight:QueryDashboard", 
                            "quicksight:ListDashboardVersions"] 
        elif(typ.upper() == "ANALYSIS"):
            perms = ["quicksight:RestoreAnalysis", 
                        "quicksight:UpdateAnalysisPermissions", 
                        "quicksight:DeleteAnalysis", 
                        "quicksight:QueryAnalysis", 
                        "quicksight:DescribeAnalysisPermissions", 
                        "quicksight:DescribeAnalysis", 
                        "quicksight:UpdateAnalysis"]
        
        return perms
    
    def __make_permissions_list_qs(self, username_list, typ, region):
        permissions_list = []
        for user in username_list:
            perms_dict = {'Principal': f'arn:aws:quicksight:{region}:{wr.sts.get_account_id()}:user/default/{user}',
                          'Actions': self.__get_permissions_type_qs(typ)}
            permissions_list.append(perms_dict)

        return permissions_list
    
    def __get_permissions_qs(self, username_list, typ, region):
        permissions = self.__make_permissions_list_qs(username_list, typ.upper(), region)
        return permissions
    
    @staticmethod
    def grant_permission_lf(arn, database_name, permission_list):
        try:
            response = client_lf.grant_permissions(
            Principal={
                'DataLakePrincipalIdentifier': arn
            },
            Resource={
                'Table': {
                    'DatabaseName': database_name,
                    'TableWildcard': {}

                },
            },
            Permissions=permission_list)
        except Exception as e:
            print(e)
            print("Cannot add role/user to Lake Formation")
            raise e
        return response

    # QS USERS FUNCTIONS
    # ----------------------------
    @staticmethod
    def get_all_users_qs():
        try:
            admin_list = []
            user_list = []
            user_list_all = wr.quicksight.list_users()
            for user in user_list_all:
                if(user['Role'].upper() == 'ADMIN'):
                    admin_list.append(user['UserName'])
                else:
                    user_list.append(user['UserName'])
            return admin_list, user_list
        except Exception as e:
            print(e)
            print("Could not get all users")


    # QS DATA SOURCE FUNCTIONS
    # ----------------------------
    @staticmethod
    def create_data_source(data_source_name:str, username_list:list):
        try:
            wr.quicksight.create_athena_data_source(
                name=data_source_name,
                allowed_to_manage=username_list  
            )
            print(f"Athena Data Source: {data_source_name} Created")
            return
        except Exception as e:
            print(e)
            print(f"Data source creation failed for {data_source_name}")
            raise e

    @staticmethod
    def delete_all_quicksight_data_sources():
        try:
            wr.quicksight.delete_all_data_sources()
            print(f"All QuickSight Data Sources Deleted")
            return
        except Exception as e:
            print(e)
            print("Delete failed for all quicksight data sources")
            raise e

    @staticmethod
    def delete_quicksight_data_source(data_source_name:str):
        try:
            wr.quicksight.delete_data_source(name=data_source_name)
            print(f"Data Source: {data_source_name} Deleted")
            return
        except Exception as e:
            print(e)
            print("Delete failed for all datasets")
            raise e

    # QS DATASET FUNCTIONS
    # ----------------------------
    @staticmethod
    def create_data_set(dataset_name:str, glue_db:str, glue_table:str, data_source_name:str, username_list:list):
        try:
            dataset_id = wr.quicksight.create_athena_dataset(
                name=dataset_name,
                database=glue_db,
                table=glue_table,
                data_source_name=data_source_name,
                allowed_to_manage=username_list)
            print(f"Dataset ID: {dataset_id}, Dataset Name:{dataset_name} Created")
            return
        except Exception as e:
            print(e)
            print(f"Dataset creation failed for {dataset_name}")
            raise e

    @staticmethod
    def delete_all_quicksight_datasets():
        try:
            wr.quicksight.delete_all_datasets()
            print(f"All QuickSight Datasets Deleted")
            return
        except Exception as e:
            print(e)
            print("Delete failed for all datasets")
            raise e

    @staticmethod
    def delete_quicksight_dataset(dataset_name:str):
        try:
            wr.quicksight.delete_dataset(name=dataset_name)
            print(f"Dataset: {dataset_name} Deleted")
            return
        except Exception as e:
            print(e)
            print("Delete failed for all datasets")
            raise e

    # QS TEMPLATE FUNCTIONS
    # ----------------------------
    @staticmethod
    def delete_all_quicksight_templates():
        try:
            wr.quicksight.delete_all_templates()
            print(f"All QuickSight Templates Deleted")
            return
        except Exception as e:
            print(e)
            print("Delete failed for all quicksight templates")
            raise e

    def create_template_from_analysis(self, template_name:str, username_list:str, region:str, dataset_name:str, analysis_name: str):
        try:
            response = client.create_template(
                AwsAccountId=wr.sts.get_account_id(),
                TemplateId=template_name,
                Name=template_name,
                Permissions=self.__get_permissions_qs(username_list, "TEMPLATE", region),
                SourceEntity={
                    'SourceAnalysis': {
                        'Arn': f'arn:aws:quicksight:{region}:{wr.sts.get_account_id()}:analysis/{analysis_name}',
                        'DataSetReferences': [
                            {
                                'DataSetPlaceholder': f'{dataset_name}',
                                'DataSetArn': f'arn:aws:quicksight:{region}:{wr.sts.get_account_id()}:dataset/{wr.quicksight.get_dataset_id(name=dataset_name)}'
                            }, 
                        ]
                    },
                })
            return response
        except Exception as e:
            print(e)
            print("Create template failed")
            raise e

    def copy_template_from_amc_template(self, template_name:str, username_list:str, region:str, template_arn:str):
        try:
            response = client.create_template(
                AwsAccountId=wr.sts.get_account_id(),
                TemplateId=template_name,
                Name=template_name,
                Permissions=self.__get_permissions_qs(username_list, "TEMPLATE", region),
                SourceEntity={'SourceTemplate': {
                'Arn': template_arn
            }})
            return response
        except Exception as e:
            print(e)
            print("Create template failed")
            raise e

    @staticmethod
    def create_public_template(template_name:str, region:str, dataset_name:str, analysis_name: str):
        try:
            response = client.create_template(
                AwsAccountId=wr.sts.get_account_id(),
                TemplateId=template_name,
                Name=template_name,
                Permissions=[{'Principal': '*',
                          'Actions': [ 
    #                   "quicksight:DescribeTemplatePermissions", 
    #                   "quicksight:DescribeTemplateAlias", 
    #                   "quicksight:ListTemplateAliases", 
                      "quicksight:ListTemplates", 
                      #"quicksight:ListTemplateVersions", 
                      "quicksight:DescribeTemplate"
                  ]}],
                SourceEntity={
                    'SourceAnalysis': {
                        'Arn': f'arn:aws:quicksight:{region}:{wr.sts.get_account_id()}:analysis/{analysis_name}',
                        'DataSetReferences': [
                            {
                                'DataSetPlaceholder': dataset_name,
                                'DataSetArn': f'arn:aws:quicksight:{region}:{wr.sts.get_account_id()}:dataset/{wr.quicksight.get_dataset_id(name=dataset_name)}'
                            }, 
                        ]
                    },
                })
            return response
        except Exception as e:
            print(e)
            print("Create public template failed")
            raise e
    
    @staticmethod
    def delete_quicksight_template(template_name:str):
        try:
            wr.quicksight.delete_template(name=template_name)
            print(f"Template: {template_name} Deleted")
            return
        except Exception as e:
            print(e)
            print("Create public template failed")
            raise e

    # QS Analysis FUNCTIONS
    # ---------------------------- 
    def create_analysis_from_template(self, accountid, analysis_name, username_list, dataset_name, region, template_name):
        try:
            response = client.create_analysis(
                AwsAccountId=accountid,
                AnalysisId= analysis_name,
                Name=analysis_name,
                Permissions= self.__get_permissions_qs(username_list, "ANALYSIS", region),
                SourceEntity={
                    'SourceTemplate': {
                        'DataSetReferences': [
                            {
                                'DataSetPlaceholder': dataset_name,
                                'DataSetArn': f'arn:aws:quicksight:{region}:{accountid}:dataset/{wr.quicksight.get_dataset_id(name=dataset_name)}'
                            },
                        ],
                        'Arn': f'arn:aws:quicksight:{region}:{accountid}:template/{template_name}'
                    }
                }
            )
            return response
        except Exception as e:
            print(e)
            print("Create template from analysis failed")
            raise e
    
    @staticmethod
    def delete_quicksight_analysis(accountid:str, analysis_name:str, region:str):
        try:
            response = client.delete_analysis(
                AwsAccountId=accountid,
                AnalysisId=analysis_name
            )
            print(f"Analysis: {analysis_name} Deleted")
            return response
        except Exception as e:
            print(e)
            print("Create template from analysis failed")
            raise e
    
        
    
    # QS DASHBOARD FUNCTIONS
    # ---------------------------- 
    @staticmethod
    def check_dashboard_exists(dashboard_name:str):
        try:
            my_id = wr.quicksight.get_dashboard_id(name=dashboard_name)
            print(f"Dashboard: {dashboard_name} exists")
            return
        except Exception as e:
            print(e)
            print("Dashboard doesnt exists")
        
    
    def create_dashboard_from_template(self, dashboard_name:str, username_list:str, region:str, dataset_name:str, template_name:str, accountid:str):
        try:
            response = client.create_dashboard(
                    AwsAccountId=accountid,
                    DashboardId=dashboard_name,
                    Name=dashboard_name,
                    Permissions=self.__get_permissions_qs(username_list, "DASHBOARD", region),
                    SourceEntity={
                        'SourceTemplate': {
                            'DataSetReferences': [
                                {
                                    'DataSetPlaceholder': dataset_name,
                                    'DataSetArn': f'arn:aws:quicksight:{region}:{accountid}:dataset/{wr.quicksight.get_dataset_id(name=dataset_name)}'
                                },
                            ],
                            'Arn': f'arn:aws:quicksight:{region}:{accountid}:template/{wr.quicksight.get_template_id(name=template_name)}'
                        }
                    }

                )
            return response
        except Exception as e:
            print(e)
            print("Dashboard creation failed")
            raise e
    
    @staticmethod
    def delete_all_quicksight_dashboards():
        try:
            wr.quicksight.delete_all_dashboards()
            print(f"All Dashboards Deleted")
            return
        except Exception as e:
            print(e)
            print("Delete failed for all quicksight dashboards")
            raise e

    def update_dashboard_permissions(self, typ:str, dashboard_name:str, username_list:str, dashboard_user_type:str, region:str):
        if(typ.upper() == "GRANT"):
            response = client.update_dashboard_permissions(
            AwsAccountId=wr.sts.get_account_id(),
            DashboardId=dashboard_name,
            GrantPermissions=self.__get_permissions_qs(username_list, dashboard_user_type, region),
        )
        elif(typ.upper() == "REVOKE"):
            response = client.update_dashboard_permissions(
            AwsAccountId=wr.sts.get_account_id(),
            DashboardId=dashboard_name,
            RevokePermissions=self.__get_permissions_qs(username_list, dashboard_user_type, region)
        )
        print(f"Dashboard Name: {dashboard_name} Updated for Users: {username_list}")
        return
    
    @staticmethod
    def delete_quicksight_dashboard(dashboard_name:str):
        try:
            wr.quicksight.delete_dashboard(name=dashboard_name)
            print(f"Dashboard: {dashboard_name} Deleted")
            return
        except Exception as e:
            print(e)
            print("Create public template failed")
            raise e
