/*
 * test_recovery_log.c -- unit test for undo log recovery
 */
#include "unittest.h"
#include "util.h"
#include "valgrind_internal.h"

#define TEST_VALUE_A 5
#define TEST_VALUE_B 10
#define TEST_VALUE_C 15

TOID_DECLARE_ROOT(struct test_obj);

struct test_obj {
	int a;
	int b;
	int c;
};


// pmreorder_create_pool -- create a obj pool before recording ops
//
// Two parameters required
// 1 - the path to the testfile
// 2 - The layout of the pool
static void
 pmreorder_create_pool(char* file, char* layout){
       //Create theobject pool
        PMEMobjpool *pop;
        if ((pop = pmemobj_create(file, layout, PMEMOBJ_MIN_POOL, 777)) == NULL) {
                printf("Error : Exists");
                UT_FATAL("!pmemobj_create : %s", file);
        }
	
	TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));
        //POBJ_ZNEW(pop, &obj, struct test_obj);

        D_RW(obj)->a = 1;
        D_RW(obj)->b = 1;
        D_RW(obj)->c = 1;

	printf("Done creating pool");
	pmemobj_close(pop);
}


static void
do_transaction(char* file)
{
	//Open the object pool
	PMEMobjpool *pop;
	char *layout = "ulog_test"; 
       	if ((pop = pmemobj_open(file, layout)) == NULL) {
                printf("Error :File doesn't exist");
                UT_FATAL("!pmemobj_open : %s", file);
        }

	//PMEMoid root = pmemobj_root(pop, sizeof (struct test_obj));
	//struct test_obj *obj = (struct test_obj*)pmemobj_direct(root);	
	TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

	printf("Values before tx are a:%d, b:%d, c:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
	VALGRIND_EMIT_LOG("WORKLOAD.BEGIN");
	VALGRIND_EMIT_LOG("TRANSACTION.BEGIN");
	TX_BEGIN(pop) {
		TX_ADD(obj);
		D_RW(obj)->a = TEST_VALUE_A;
		D_RW(obj)->b = TEST_VALUE_B;
		D_RW(obj)->c = TEST_VALUE_C;
		VALGRIND_EMIT_LOG("TRANSACTION_CRASH.BEGIN");
		pmemobj_tx_commit();
	} 		
	TX_END
	VALGRIND_EMIT_LOG("TRANSACTION_CRASH.END");
	VALGRIND_EMIT_LOG("TRANSACTION.END");
	VALGRIND_EMIT_LOG("WORKLOAD.END");

	//printf("Values are a:%d, b:%d, c:%d", D_RW(obj)->a, D_RW(obj)->b, D_RW(obj)->c);
	pmemobj_close(pop);
}

static int 
check_consistency(char *file) {
//check_values(TOID(struct test_obj) *obj) {
	int consistency = 0;
	
	//open the object pool and get the object
	PMEMobjpool *pop;
	char *layout = "ulog_test";
	if ((pop = pmemobj_open(file, layout)) == NULL)
		UT_FATAL("!pmemobj_open : %s", file);

	//PMEMoid root = pmemobj_root(pop, sizeof (struct test_obj));
	//struct test_obj *obj = (struct test_obj*)pmemobj_direct(root);

	//struct test_obj *obj = (struct test_obj*)pop;
        TOID(struct test_obj) obj = (TOID(struct test_obj))pmemobj_root(pop, sizeof (struct test_obj));

	if (D_RW(obj)->a != TEST_VALUE_A || D_RW(obj)->b != TEST_VALUE_B || D_RW(obj)->c != TEST_VALUE_C) {
		printf("\nInconsistency detected");
		printf("\na:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);	
		printf("\nShould have been a:%d, b:%d, c:%d\n", TEST_VALUE_A, TEST_VALUE_B, TEST_VALUE_C);	
		consistency = 1;
	} else {

	char str[1000];
	sprintf(str, "\n Check consistency -> a:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
	//printf("String is : %s", str);
	VALGRIND_EMIT_LOG(str);
	VALGRIND_EMIT_LOG("Hello");
	printf("\nCOnsistent : Values -> a:%d, b:%d, c:%d", D_RO(obj)->a, D_RO(obj)->b, D_RO(obj)->c);
	}
	
	pmemobj_close(pop);
	return consistency;
}

int
main(int argc, char *argv[])
{
	START(argc, argv, "obj_tx_flow");

	util_init();

	char* layout;
	//remove(argv[2]);

	if ((argc < 3) || (strchr("ptc", argv[1][0]) == NULL) || argv[1][1] != '\0')
		UT_FATAL("usage: %s p|t|c [file]", argv[0]);
	
	if((argv[1][0] == 'p') && (argc !=4)) 
		UT_FATAL("usage: %s p [file] [layout]", argv[0]);
	
	if((argv[1][0] == 'p') && (argc ==4)) 
		layout = argv[3];
	
 	char opt = argv[1][0];

	switch (opt) {
		case 'p':
			printf("\n Creating object pool\n");
			pmreorder_create_pool(argv[2], layout);
			break;
		case 't':
			printf("\nPerform Transaction\n");
			do_transaction(argv[2]);
		
			break;
		case 'c':

			printf("\nPerform Consistency check\n");
			return check_consistency(argv[2]); 
		default:
			UT_FATAL("Incorrect option %c", opt);
	}


	DONE(NULL);
}
