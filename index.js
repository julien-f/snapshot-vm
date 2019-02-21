#!/usr/bin/env node

process.env.DEBUG = "*";

const defer = require("golike-defer").default;
const asyncMap = require("@xen-orchestra/async-map").default;
const { ignoreErrors, pDelay } = require("promise-toolbox");
const { createClient, isOpaqueRef, NULL_REF, Xapi } = require("xen-api");

const pRetry = require("./_pRetry");

// const OPAQUE_REF_RE = /OpaqueRef:[0-9a-z-]+/;
// const extractOpaqueRef = str => {
//   const matches = OPAQUE_REF_RE.exec(str);
//   if (!matches) {
//     throw new Error("no opaque ref found");
//   }
//   return matches[0];
// };

const isValidRef = ref => ref !== NULL_REF && isOpaqueRef(ref);

async function getVmDisks(xapi, vm) {
  const disks = { __proto__: null };
  await asyncMap(xapi.getRecords("VBD", vm.VBDs), async vbd => {
    let vdiRef;
    if (vbd.type === "Disk" && isValidRef((vdiRef = vbd.VDI))) {
      const vdi = await xapi.getRecord("VDI", vdiRef);
      disks[vdi.$id] = vdi;
    }
  });
  return disks;
}

Xapi.prototype.getRecords = function(type, refs) {
  return Promise.all(refs.map(ref => this.getRecord(type, ref)));
};

Xapi.prototype._deleteVm = async function _deleteVm(
  vm,
  deleteDisks = true,
  force = false,
  forceDeleteDefaultTemplate = false
) {
  const { $ref } = vm;

  // ensure the vm record is up-to-date
  vm = await this.getRecord("VM", $ref);

  if (!force && "destroy" in vm.blocked_operations) {
    throw new Error("destroy is blocked");
  }

  if (
    !forceDeleteDefaultTemplate &&
    vm.other_config.default_template === "true"
  ) {
    throw new Error("VM is default template");
  }

  // It is necessary for suspended VMs to be shut down
  // to be able to delete their VDIs.
  if (vm.power_state !== "Halted") {
    await this.call("VM.hard_shutdown", $ref);
  }

  await Promise.all([
    this.call("VM.set_is_a_template", vm.$ref, false),
    this.setFieldEntries(vm, "blocked_operations", {
      destroy: null,
    }),
    this.setFieldEntries(vm, "other_config", {
      default_template: null,
    }),
  ]);

  // must be done before destroying the VM
  const disks = await getVmDisks(this, vm);

  // this cannot be done in parallel, otherwise disks and snapshots will be
  // destroyed even if this fails
  await this.call("VM.destroy", $ref);

  return Promise.all([
    ignoreErrors.call(
      asyncMap(this.getRecords("VM", vm.snapshots), snapshot =>
        this._deleteVm(snapshot)
      )
    ),

    deleteDisks &&
      ignoreErrors.call(
        asyncMap(disks, ({ $ref: vdiRef }) => {
          let onFailure = () => {
            onFailure = Function.prototype;
            // onFailure = vdi => {
            //   vdi.$VBDs.forEach(vbd => {
            //     if (vbd.VM !== $ref) {
            //       const vm = vbd.$VM;
            //     }
            //   });
            // };

            // maybe the control domain has not yet unmounted the VDI,
            // check and retry after 5 seconds
            return pDelay(5e3).then(test);
          };
          const test = async () => {
            const vdi = await this.getRecord("VDI", vdiRef);
            return (
              // Only remove VBDs not attached to other VMs.
              vdi.VBDs.length < 2 ||
                (await this.getRecords("VBD", vdi.VBDs)).every(
                  vbd => vbd.VM === $ref
                )
                ? this.call("VDI.destroy", vdi.$ref)
                : onFailure(vdi)
            );
          };
          return test();
        })
      ),
  ]);
};

Xapi.prototype.snapshotVm = async function(vm, nameLabel = vm.name_label) {
  const vmRef = vm.$ref;
  let ref;
  do {
    if (!vm.tags.includes("xo-disable-quiesce")) {
      try {
        vm = await this.getRecord("VM", vmRef);
        ref = await pRetry(
          async bail => {
            try {
              return await this.call(
                // $cancelToken,
                "VM.snapshot_with_quiesce",
                vmRef,
                nameLabel
              );
            } catch (error) {
              if (
                error == null ||
                error.code !== "VM_SNAPSHOT_WITH_QUIESCE_FAILED"
              ) {
                throw bail(error);
              }

              // detect and remove new broken snapshots
              //
              // see https://github.com/vatesfr/xen-orchestra/issues/3936
              const prevSnapshotRefs = new Set(vm.snapshots);
              const snapshotNameLabelPrefix = `Snapshot of ${vm.uuid} [`;
              vm = await this.getRecord("VM", vmRef);
              const createdSnapshots = (await this.getRecords(
                "VM",
                vm.snapshots
              )).filter(
                _ =>
                  !prevSnapshotRefs.has(_.$ref) &&
                  _.name_label.startsWith(snapshotNameLabelPrefix)
              );

              // be safe: only delete if there was a single match
              if (createdSnapshots.length === 1) {
                ignoreErrors.call(this._deleteVm(createdSnapshots[0]));
              }

              throw error;
            }
          },
          {
            delay: 60e3,
            tries: 3,
          }
        );
        ignoreErrors.call(this.call("VM.add_tags", ref, "quiesce"));

        break;
      } catch (error) {
        const { code } = error;
        if (
          code !== "VM_SNAPSHOT_WITH_QUIESCE_NOT_SUPPORTED" &&
          // quiesce only work on a running VM
          code !== "VM_BAD_POWER_STATE" &&
          // quiesce failed, fallback on standard snapshot
          // TODO: emit warning
          code !== "VM_SNAPSHOT_WITH_QUIESCE_FAILED"
        ) {
          throw error;
        }
      }
    }
    ref = await this.call(
      // $cancelToken,
      "VM.snapshot",
      vmRef,
      nameLabel
    );
  } while (false);

  // Convert the template to a VM and wait to have receive the up-
  // to-date object.
  const [, snapshot] = await Promise.all([
    this.call("VM.set_is_a_template", ref, false),
    this.getRecord("VM", ref),
  ]);

  return snapshot;
};

defer(async ($defer, args) => {
  if (args.length < 1) {
    return console.log("Usage: import-vm <XS URL> <VM UUID>");
  }

  const xapi = createClient({
    allowUnauthorized: true,
    url: args[0],
    watchEvents: false,
  });

  await xapi.connect();
  $defer(() => xapi.disconnect());

  console.log(await xapi.snapshotVm(await xapi.getRecordByUuid("VM", args[1])));
})(process.argv.slice(2)).catch(console.error.bind(console, "error"));
