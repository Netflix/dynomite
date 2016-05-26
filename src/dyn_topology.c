#include "dyn_core.h"
#include "dyn_topology.h"

dictType dc_string_dict_type = {
		dict_string_hash,            /* hash function */
		NULL,                        /* key dup */
		NULL,                        /* val dup */
		dict_string_key_compare,     /* key compare */
		dict_string_destructor,      /* key destructor */
		NULL                         /* val destructor */
};

static rstatus_t
rack_init(struct rack *rack)
{
	rack->continuum = dn_alloc(sizeof(struct continuum));
	rack->ncontinuum = 0;
	rack->nserver_continuum = 0;
	rack->name = dn_alloc(sizeof(struct string));
	string_init(rack->name);

	rack->dc = dn_alloc(sizeof(struct string));
	string_init(rack->dc);

	return DN_OK;
}


static rstatus_t
rack_deinit(struct rack *rack)
{
	if (rack->continuum != NULL) {
		dn_free(rack->continuum);
	}

	return DN_OK;
}

static rstatus_t
dc_init(struct datacenter *dc)
{
	rstatus_t status;

	dc->dict_rack = dictCreate(&dc_string_dict_type, NULL);
	dc->name = dn_alloc(sizeof(struct string));
	string_init(dc->name);
    dc->preselected_rack_for_replication = NULL;

	status = array_init(&dc->racks, 3, sizeof(struct rack));

	return status;
}

static rstatus_t
rack_destroy(void *elem, void *data)
{
	struct rack *rack = elem;
	return rack_deinit(rack);
}

static rstatus_t
dc_deinit(struct datacenter *dc)
{
	array_each(&dc->racks, rack_destroy, NULL);
	string_deinit(dc->name);
	//dictRelease(dc->dict_rack);
	return DN_OK;
}

rstatus_t
datacenter_destroy(void *elem, void *data)
{
	struct datacenter *dc = elem;
	dc_deinit(dc);
	return DN_OK;
}



struct datacenter *
topo_get_dc(struct topology *topo, struct string *dcname)
{
	struct datacenter *dc;
	uint32_t i, len;

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_dc dc '%.*s'",
				dcname->len, dcname->data);
	}

	for (i = 0, len = array_n(&topo->datacenters); i < len; i++) {
		dc = (struct datacenter *) array_get(&topo->datacenters, i);
		ASSERT(dc != NULL);
		ASSERT(dc->name != NULL);

		if (string_compare(dc->name, dcname) == 0) {
			return dc;
		}
	}

	dc = array_push(&topo->datacenters);
	dc_init(dc);
	string_copy(dc->name, dcname->data, dcname->len);

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_dc about to exit dc '%.*s'",
				dc->name->len, dc->name->data);
	}

	return dc;
}

struct rack *
topo_get_rack(struct datacenter *dc, struct string *rackname)
{
	ASSERT(dc != NULL);
	ASSERT(dc->dict_rack != NULL);
	ASSERT(dc->name != NULL);

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_rack   '%.*s'", rackname->len, rackname->data);
	}
	/*
   struct rack *rack = dictFetchValue(dc->dict_rack, rackname);
   if (rack == NULL) {
      rack = array_push(&dc->racks);
      rack_init(rack);
      string_copy(rack->name, rackname->data, rackname->len);
      string_copy(rack->dc, dc->name->data, dc->name->len);
      rack->continuum = dn_alloc(sizeof(struct continuum));

   	dictAdd(dc->dict_rack, rackname, rack);
   }
	 */

	struct rack *rack;
	uint32_t i, len;
	for (i = 0, len = array_n(&dc->racks); i < len; i++) {
		rack = (struct rack *) array_get(&dc->racks, i);

		if (string_compare(rack->name, rackname) == 0) {
			return rack;
		}
	}

	rack = array_push(&dc->racks);
	rack_init(rack);
	string_copy(rack->name, rackname->data, rackname->len);
	string_copy(rack->dc, dc->name->data, dc->name->len);

	if (log_loggable(LOG_DEBUG)) {
		log_debug(LOG_DEBUG, "server_get_rack exiting  '%.*s'",
				rack->name->len, rack->name->data);
	}

	return rack;
}

void
topo_print(struct topology *t)
{
    log_debug(LOG_VERB, "Peers Datacenters/racks/nodes .................................................");
    uint32_t dc_index, dc_len;
    for(dc_index = 0, dc_len = array_n(&t->datacenters); dc_index < dc_len; dc_index++) {
        struct datacenter *dc = array_get(&t->datacenters, dc_index);
        log_debug(LOG_VERB, "Peer datacenter........'%.*s'", dc->name->len, dc->name->data);
        uint32_t rack_index, rack_len;
        for(rack_index=0, rack_len = array_n(&dc->racks); rack_index < rack_len; rack_index++) {
            struct rack *rack = array_get(&dc->racks, rack_index);
            log_debug(LOG_VERB, "\tPeer rack........'%.*s'", rack->name->len, rack->name->data);
            log_debug(LOG_VERB, "\tPeer rack ncontinuumm    : %d", rack->ncontinuum);
            log_debug(LOG_VERB, "\tPeer rack nserver_continuum    : %d", rack->nserver_continuum);
        }
    }
	log_debug(LOG_VERB, "...............................................................................");

}

struct topology *
topo_create(void)
{
    struct topology *t = dn_zalloc(sizeof(struct topology));
    array_null(&t->datacenters);
    array_init(&t->datacenters, 1, sizeof(struct datacenter));
    string_init(&t->seed_provider);
    return t;
}
