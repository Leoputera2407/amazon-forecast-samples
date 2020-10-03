class ResourcePending(Exception):
    pass


class ResourceFailed(Exception):
    pass


def take_action(status):
    if isinstance(status, list):
        for stat in status:
            if stat in {'CREATE_PENDING', 'CREATE_IN_PROGRESS'}:
                raise ResourcePending
        #If both stats are neither pending/in_progress nor active, then fail
        for stat in status:
            if stat != 'ACTIVE':
                raise ResourceFailed
    else:
        if status in {'CREATE_PENDING', 'CREATE_IN_PROGRESS'}:
            raise ResourcePending
        if status != 'ACTIVE':
            raise ResourceFailed
    return True

def take_action_delete(status):
    if isinstance(status, list):
        for stat in status:
            if stat in {'DELETE_PENDING', 'DELETE_IN_PROGRESS'}:
                raise ResourcePending
    else:
        if status in {'DELETE_PENDING', 'DELETE_IN_PROGRESS'}:
            raise ResourcePending
    raise ResourceFailed

