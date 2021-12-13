import re
from enum import Enum
from typing import List

import pandas as pd
from firecloud import api as fapi
from firecloud.errors import FireCloudServerError

from .utils import *

logger = get_configured_logger(log_level=logging.INFO)


########################################################################################################################
def fetch_existing_root_table(ns: str, ws: str, etype: str) -> pd.DataFrame:
    response = fapi.get_entities(ns, ws, etype=etype)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    entities = [e.get('name') for e in response.json()]
    entity_type = [e.get('entityType') for e in response.json()][0]
    attributes = pd.DataFrame.from_dict([e.get('attributes') for e in response.json()]).sort_index(axis=1).astype('str')
    attributes.insert(0, column=entity_type, value=entities)
    return attributes.copy(deep=True)


def upload_root_table(ns: str, ws: str, table: pd.DataFrame) -> None:
    """
    Upload root level data table (assumed to be correctly formatted) to Terra ns/ws.
    Most useful when initializing a workspace.
    """
    response = fapi.upload_entities(namespace=ns,
                                    workspace=ws,
                                    entity_data=table.to_csv(sep='\t', index=False),
                                    model='flexible')
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)


########################################################################################################################
class MembersOperationType(Enum):
    RESET = 1  # remove old members and fill with with new members
    MERGE = 2  # just add in new members that weren't there


def upload_set_table(ns: str, ws: str, table: pd.DataFrame,
                     desired_set_type_name: str, membership_col_name: str,
                     operation: MembersOperationType) -> None:
    """
    Upload set level table to Terra ns/ws.
    Table is not expected to be formatted ready for upload.
    However, the column names are expected to be formatted in a way described in `format_set_table_ready_for_upload`
    :param ns:
    :param ws:
    :param table:
    :param desired_set_type_name:
    :param membership_col_name:
    :param operation: whether old members list (if any) needs to be reset, or just add new ones.
    :return:
    """
    formatted_set_table, members_for_each_set = \
        format_set_table_ready_for_upload(table, desired_set_type_name, membership_col_name)

    # upload set table, except membership column
    response = fapi.upload_entities(namespace=ns, workspace=ws,
                                    entity_data=formatted_set_table.to_csv(sep='\t', index=False),
                                    model='flexible')
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)
    logger.info("uploaded set level table, next fill-in members...")
    
    # update each set with its members
    member_entity_type = re.sub("s$", "", membership_col_name)
    for i in range(len(members_for_each_set)):
        set_uuid = formatted_set_table.iloc[i, 0]
        members = members_for_each_set[i]
        try:
            __upload_one_set(ns, ws, etype=desired_set_type_name, ename=set_uuid,
                             member_type=member_entity_type, members=members, operation=operation)
        except FireCloudServerError:
            logger.error(f"Failed to upload membership information for {set_uuid}")
            raise


def format_set_table_ready_for_upload(set_table: pd.DataFrame, desired_set_type_name: str,
                                      membership_col_name: str) -> (pd.DataFrame, List[List[str]]):
    """
    Given a set table of the format [entity:<blah>_id, ... , membership_col_name, ...]
    where each cell in the membership_col_name is expected to be a list of strings, i.e. uuids of the members.
    :param set_table: to-be-formatted table
    :param desired_set_type_name: desired name of the table, i.e. its 0th column will be f"entity:{desired_set_type_name}_id"
    :param membership_col_name: name of the column holding the members
    :return: a formatted table that is ready to be uploaded to Terra via API calls
    """
    old_uuid_col_name = set_table.columns[0]
    new_uuid_col_name = re.sub(old_uuid_col_name, f"entity:{desired_set_type_name}_id", old_uuid_col_name)
    formatted_set_table = set_table.rename({old_uuid_col_name: new_uuid_col_name}, axis=1)

    members = formatted_set_table[membership_col_name].tolist()

    formatted_set_table.drop([membership_col_name], axis=1, inplace=True)

    return formatted_set_table, members


def __upload_one_set(ns: str, ws: str,
                     etype: str, ename: str,
                     member_type: str, members: List[str],
                     operation: MembersOperationType) -> None:
    """
    For a given set identified by etype and ename, fill-in it's members,
    assuming the member entities already exists on Terra.
    :param ns: namespace
    :param ws: workspace
    :param etype: entity type
    :param ename: entity UUID
    :param member_type: entity type of the members
    :param members: list of member uuids
    :param operation: whether to override or append to existing membership list
    :return:
    """

    operations = list()
    response = fapi.get_entity(ns, ws, etype, ename)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    attributes = response.json().get('attributes')
    if f'{member_type}s' not in attributes:
        operations.append({
            "op": "CreateAttributeEntityReferenceList",
            "attributeListName": f"{member_type}s"
        })
        members_to_upload = members
    else:
        old_members = [e['entityName'] for e in attributes[f'{member_type}s']['items']]
        if operation == MembersOperationType.MERGE:
            members_to_upload = list(set(members) - set(old_members))
        else:
            for member_id in old_members:
                operations.append({
                    "op": "RemoveListMember",
                    "attributeListName": f"{member_type}s",
                    "removeMember": {"entityType":f"{member_type}",
                                     "entityName":f"{member_id}"}
                })
            members_to_upload = members

    for member_id in members_to_upload:
        operations.append({
            "op": "AddListMember",
            "attributeListName": f"{member_type}s",
            "newMember": {"entityType":f"{member_type}",
                          "entityName":f"{member_id}"}
        })
    logger.debug(operations)

    response = fapi.update_entity(ns, ws,
                                  etype=etype,
                                  ename=ename,
                                  updates=operations)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)


def fetch_and_format_existing_set_table(ns: str, ws: str, etype: str, member_column_name: str) -> pd.DataFrame:
    """
    Intended to be used when some columns of an existing set level table are to be edited.
    See add_or_drop_columns_to_existing_set_table() for example
    :param ns:
    :param ws:
    :param etype:
    :param member_column_name:
    :return:
    """
    # fetch and keep all attributes in original table
    response = fapi.get_entities(ns, ws, etype=etype)

    entities = pd.Series([e.get('name') for e in response.json()], name=f"entity:{etype}_id")
    attributes = pd.DataFrame.from_dict([e.get('attributes') for e in response.json()])

    # re-format the membership column, otherwise uploading will cause problems
    x = attributes[member_column_name].apply(lambda d: [e.get('entityName') for e in d.get('items')])
    attributes[member_column_name] = x

    return pd.concat([entities, attributes], axis=1)


def add_or_drop_columns_to_existing_set_table(ns: str, ws: str, etype: str, member_column_name: str) -> None:
    """
    An example (so please don't run) scenario to use fetch_and_format_existing_set_table.
    :param ns:
    :param ws:
    :param etype:
    :param member_column_name:
    :return:
    """

    formatted_original_table = fetch_and_format_existing_set_table(ns, ws, etype, member_column_name)

    # an example: do something here, add, drop, batch-modify existing columns
    identities = formatted_original_table.iloc[:, 1].apply(lambda s: s)
    identities.name = 'identical_copy_of_col_2'
    updated_table = pd.concat([formatted_original_table, identities], axis=1)

    # and upload
    upload_set_table(ns, ws,
                     updated_table,
                     desired_set_type_name=etype, membership_col_name=member_column_name,
                     operation=MembersOperationType.RESET)


########################################################################################################################
def transfer_set_table(namespace: str,
                       original_workspace: str, new_workspace: str,
                       original_set_type: str, membership_col_name: str,
                       desired_new_set_type_name: str) -> None:
    """
    Transfer set-level table from one workspace to another workspace.
    It's assumed that
      * the two workspaces live under the same namespace
      * the membership column are exactly the same
      * all the member entities are already in the target workspace
    though these assumptions could be relaxed later.
    :param namespace:
    :param original_workspace:
    :param new_workspace:
    :param original_set_type:
    :param membership_col_name:
    :param desired_new_set_type_name:
    :return:
    """

    # fetch
    response = fapi.get_entities(namespace,
                                 original_workspace,
                                 etype=original_set_type)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)
    logger.info(f"Original set table {original_set_type} fetched")

    # format
    uuids = [e.get('name') for e in response.json()]
    attributes_table = pd.DataFrame.from_dict([e.get('attributes') for e in response.json()])
    attributes_table.insert(0, f'entity:{original_set_type}_id', uuids)
    original_table = attributes_table.copy(deep=True)

    ready_for_upload_table, members_list = format_set_table_ready_for_upload(
        original_table, desired_set_type_name=desired_new_set_type_name, membership_col_name=membership_col_name)

    # everything except membership
    response = fapi.upload_entities(namespace, new_workspace,
                                    entity_data=ready_for_upload_table.to_csv(sep='\t', index=False),
                                    model='flexible')
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)
    logger.info("uploaded set level table, next fill-in members...")

    # update each set with its members
    flat_text_membership = list(map(lambda dl: [d.get('entityName') for d in dl.get('items')], members_list))
    member_entity_type = re.sub("s$", "", membership_col_name)
    for i in range(len(flat_text_membership)):
        set_uuid = ready_for_upload_table.iloc[i, 0]
        members = flat_text_membership[i]
        try:
            __upload_one_set(namespace, new_workspace,
                             etype=desired_new_set_type_name, ename=set_uuid,
                             member_type=member_entity_type, members=members, operation=MembersOperationType.RESET)
        except FireCloudServerError:
            logger.error(f"Failed to upload membership information for {set_uuid}")
            raise


########################################################################################################################
def new_or_overwrite_attribute(ns: str, ws: str,
                               etype: str, ename: str,
                               attribute_name: str, attribute_value,
                               dry_run: bool = False) -> None:
    """
    Add a new, or overwrite existing value of an attribute to a given entity, with the given value.
    :param ns: namespace
    :param ws: workspace
    :param etype: entity type
    :param ename: entity uuid
    :param attribute_name:
    :param attribute_value:
    :param dry_run: safe measure, you may want to see the command before actually committing the action.
    """

    response = fapi.get_entity(ns, ws, etype, ename)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    cov = {"op":                 "AddUpdateAttribute",
           "attributeName":      attribute_name,
           "addUpdateAttribute": attribute_value}
    operations = [cov]
    if dry_run:
        print(operations)
        return

    response = fapi.update_entity(ns, ws,
                                  etype=etype,
                                  ename=ename,
                                  updates=operations)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)


def delete_attribute(ns: str, ws: str,
                     etype: str, ename: str,
                     attribute_name: str,
                     dry_run: bool = False) -> None:
    """
    Delete a requested attribute of the requested entity
    :param ns: namespace
    :param ws: workspace
    :param etype: entity type
    :param ename: entity uuid
    :param attribute_name: name of the attribute to delete
    :param dry_run: safe measure, you may want to see the command before actually committing the action.
    """

    response = fapi.get_entity(ns, ws, etype, ename)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    action = {"op":                 "RemoveAttribute",
              "attributeName":      attribute_name}
    operations = [action]
    if dry_run:
        print(operations)
        return

    response = fapi.update_entity(ns, ws,
                                  etype=etype,
                                  ename=ename,
                                  updates=operations)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)


# this can be applied, per-row, to a table
def __update_one_list_attribute(ns: str, ws: str,
                                etype: str, ename: str,
                                attribute_name: str,
                                attribute_values: List[str],
                                operation: MembersOperationType) -> None:
    """
    To create an attribute, which must be a list of reference to something else, of the requested entity.
    Example of reference:
        1) reference to member entities
        2) reference to member entities' attribute
    Whatever the list elements refer to, the targets must exist.
    :param ns: namespace
    :param ws: workspace
    :param etype: entity type
    :param ename: entity uuid
    :param attribute_name: name the the attribute
    :param attribute_values: a list of target to referene to
    :param operation:
    :return:
    """
    operations = list()
    response = fapi.get_entity(ns, ws, etype, ename)
    if not response.ok:
        raise FireCloudServerError(response.status_code, response.text)

    attributes = response.json().get('attributes')
    if attribute_name not in attributes:  # attribute need to be created
        operations.append({
            "op": "CreateAttributeValueList",
            "attributeName": attribute_name
        })
        values_to_upload = attribute_values
    else:
        existing_values = [v for v in attributes[attribute_name]['items']]
        print(existing_values)
        if operation == MembersOperationType.MERGE:
            values_to_upload = list(set(attribute_values) - set(existing_values))
        else:
            for val in existing_values:
                operations.append({
                    "op": "RemoveListMember",
                    "attributeListName": attribute_name,
                    "removeMember": val
                })
            values_to_upload = attribute_values

    for val in values_to_upload:
        operations.append({
            "op": "AddListMember",
            "attributeListName": attribute_name,
            "newMember": val
        })
    #     logger.debug(operations)

    response = fapi.update_entity(ns, ws,
                                  etype=etype,
                                  ename=ename,
                                  updates=operations)
    if not response.ok:
        print(ename)
        raise FireCloudServerError(response.status_code, response.text)
