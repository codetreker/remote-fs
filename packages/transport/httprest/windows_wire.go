package httprest

import (
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/codetreker/remote-fs/packages/storage"
)

const OpWindows Op = "windows"
const OpWindowsControl Op = "windows-control"

func windowsControl(op storage.Operation) bool {
	switch op {
	case storage.OpWindowsState, storage.OpWindowsEnable, storage.OpWindowsQueryActivation, storage.OpWindowsSessionOpen, storage.OpWindowsSessionClose, storage.OpWindowsRenew, storage.OpWindowsStatus, storage.OpWindowsQueryAction, storage.OpWindowsCancelAction, storage.OpWindowsClose:
		return true
	}
	return false
}

func windowsReadOnly(op storage.Operation) bool {
	switch op {
	case storage.OpWindowsState, storage.OpWindowsQueryActivation, storage.OpWindowsStatus, storage.OpWindowsStat, storage.OpWindowsRead, storage.OpWindowsList, storage.OpWindowsReadLink, storage.OpWindowsQueryAction:
		return true
	}
	return false
}

func windowsMutation(op storage.Operation) bool {
	switch op {
	case storage.OpWindowsOpen, storage.OpWindowsWrite, storage.OpWindowsTruncate, storage.OpWindowsSetAttr, storage.OpWindowsRename, storage.OpWindowsSetDeletePending, storage.OpWindowsSync, storage.OpWindowsSetLink, storage.OpWindowsClose:
		return true
	}
	return false
}

func windowsActionRequired(op storage.Operation) bool {
	switch op {
	case storage.OpWindowsEnable, storage.OpWindowsQueryActivation, storage.OpWindowsOpen, storage.OpWindowsWrite, storage.OpWindowsTruncate, storage.OpWindowsSetAttr, storage.OpWindowsRename, storage.OpWindowsSetDeletePending, storage.OpWindowsLockBatch, storage.OpWindowsClose, storage.OpWindowsSetLink, storage.OpWindowsQueryAction, storage.OpWindowsCancelAction:
		return true
	}
	return false
}

type windowsOpen struct {
	Intent        storage.WindowsOpenIntent `json:"intent"`
	Lookup        storage.WindowsLookup     `json:"lookup"`
	Mode          uint32                    `json:"mode"`
	DOSAttributes uint32                    `json:"dosAttributes"`
}

func windowsOpenOf(r storage.WindowsOpenRequest) *windowsOpen {
	return &windowsOpen{Intent: r.WindowsOpenIntent, Lookup: r.Lookup, Mode: uint32(r.Mode), DOSAttributes: r.DOSAttributes}
}
func (r windowsOpen) storage() storage.WindowsOpenRequest {
	return storage.WindowsOpenRequest{WindowsOpenIntent: r.Intent, Lookup: r.Lookup, Mode: fs.FileMode(r.Mode), DOSAttributes: r.DOSAttributes}
}

type windowsRequest struct {
	Op            storage.Operation             `json:"op"`
	Session       string                        `json:"session"`
	File          string                        `json:"file"`
	Action        storage.WindowsActionID       `json:"action"`
	Options       *storage.FileSessionOptions   `json:"options,omitempty"`
	Open          *windowsOpen                  `json:"open,omitempty"`
	Offset        int64                         `json:"offset"`
	Length        int                           `json:"length"`
	Data          []byte                        `json:"data"`
	Change        *windowsAttrChange            `json:"change,omitempty"`
	Rename        *storage.WindowsRenameRequest `json:"rename,omitempty"`
	DeletePending bool                          `json:"deletePending"`
	Target        string                        `json:"target"`
	Ranges        []storage.WindowsLockRange    `json:"ranges"`
}

type windowsBasicAttr struct {
	Attr          *Attr  `json:"attr"`
	CreationTime  Time   `json:"creationTime"`
	ChangeTime    Time   `json:"changeTime"`
	DOSAttributes uint32 `json:"dosAttributes"`
	DeletePending bool   `json:"deletePending"`
}

type windowsAttr struct {
	Basic    *windowsBasicAttr       `json:"basic"`
	NameInfo storage.WindowsNameInfo `json:"nameInfo"`
}

func windowsBasicAttrOf(a storage.WindowsBasicAttr) *windowsBasicAttr {
	return &windowsBasicAttr{Attr: AttrOf(a.Attr), CreationTime: TimeOf(a.CreationTime), ChangeTime: TimeOf(a.ChangeTime), DOSAttributes: a.DOSAttributes, DeletePending: a.DeletePending}
}
func (a windowsBasicAttr) storage() storage.WindowsBasicAttr {
	return storage.WindowsBasicAttr{Attr: a.Attr.Storage(), CreationTime: a.CreationTime.Time(), ChangeTime: a.ChangeTime.Time(), DOSAttributes: a.DOSAttributes, DeletePending: a.DeletePending}
}
func windowsAttrOf(a storage.WindowsAttr) *windowsAttr {
	return &windowsAttr{Basic: windowsBasicAttrOf(a.WindowsBasicAttr), NameInfo: a.NameInfo}
}
func (a windowsAttr) storage() storage.WindowsAttr {
	return storage.WindowsAttr{WindowsBasicAttr: a.Basic.storage(), NameInfo: a.NameInfo}
}

type windowsAttrChange struct {
	Change        *AttrChange `json:"change"`
	CreationTime  *Time       `json:"creationTime,omitempty"`
	ChangeTime    *Time       `json:"changeTime,omitempty"`
	DOSAttributes *uint32     `json:"dosAttributes,omitempty"`
}

func windowsAttrChangeOf(c storage.WindowsAttrChange) *windowsAttrChange {
	value := &windowsAttrChange{Change: AttrChangeOf(c.AttrChange), DOSAttributes: c.DOSAttributes}
	if c.CreationTime != nil {
		v := TimeOf(*c.CreationTime)
		value.CreationTime = &v
	}
	if c.ChangeTime != nil {
		v := TimeOf(*c.ChangeTime)
		value.ChangeTime = &v
	}
	return value
}
func (c windowsAttrChange) storage() storage.WindowsAttrChange {
	value := storage.WindowsAttrChange{AttrChange: c.Change.Storage(), DOSAttributes: c.DOSAttributes}
	if c.CreationTime != nil {
		v := c.CreationTime.Time()
		value.CreationTime = &v
	}
	if c.ChangeTime != nil {
		v := c.ChangeTime.Time()
		value.ChangeTime = &v
	}
	return value
}

type windowsAction struct {
	Symlink          *windowsSymlink             `json:"symlink,omitempty"`
	Action           storage.WindowsActionID     `json:"action"`
	State            storage.WindowsActionState  `json:"state"`
	Attr             *windowsAttr                `json:"attr,omitempty"`
	File             string                      `json:"file"`
	CreateAction     storage.WindowsCreateAction `json:"createAction"`
	Errno            string                      `json:"errno"`
	Failure          storage.WindowsFailure      `json:"failure"`
	Applied          int                         `json:"applied"`
	HistoryRemaining time.Duration               `json:"historyRemaining"`
}

type windowsActivation struct {
	Action           storage.WindowsActionID    `json:"action"`
	State            storage.WindowsActionState `json:"state"`
	Enabled          bool                       `json:"enabled"`
	Errno            string                     `json:"errno"`
	HistoryRemaining time.Duration              `json:"historyRemaining"`
}

type windowsEntry struct {
	Name string            `json:"name"`
	Attr *windowsBasicAttr `json:"attr"`
}

type windowsResponse struct {
	bodyRelease  func()                      `json:"-"`
	Symlink      *windowsSymlink             `json:"symlink,omitempty"`
	State        *storage.WindowsState       `json:"state,omitempty"`
	Activation   *windowsActivation          `json:"activation,omitempty"`
	Session      string                      `json:"session"`
	Status       *storage.FileSessionStatus  `json:"status,omitempty"`
	File         string                      `json:"file"`
	Attr         *windowsAttr                `json:"attr,omitempty"`
	CreateAction storage.WindowsCreateAction `json:"createAction"`
	Action       *windowsAction              `json:"action,omitempty"`
	ReadAttr     *Attr                       `json:"readAttr,omitempty"`
	Data         []byte                      `json:"data"`
	Entries      []windowsEntry              `json:"entries"`
}

type windowsSymlink struct {
	Target   string                  `json:"target"`
	Location storage.WindowsNameInfo `json:"location"`
	Unparsed string                  `json:"unparsed"`
}

type windowsErrorResponse struct {
	Symlink    *windowsSymlink        `json:"symlink,omitempty"`
	Errno      string                 `json:"errno"`
	Message    string                 `json:"message"`
	Failure    storage.WindowsFailure `json:"windowsFailure,omitempty"`
	Action     *windowsAction         `json:"action,omitempty"`
	Activation *windowsActivation     `json:"activation,omitempty"`
}

func windowsErrno(name string) (syscall.Errno, error) {
	if name == "" {
		return 0, nil
	}
	value, ok := storage.ErrnoByName(name)
	if !ok {
		return 0, errors.New("Windows response has an unknown errno")
	}
	return value, nil
}
func windowsErrnoName(errno syscall.Errno) (string, error) {
	if errno == 0 {
		return "", nil
	}
	value, ok := storage.ErrnoName(errno)
	if !ok {
		return "", errors.New("Windows result has an unnameable errno")
	}
	return value, nil
}
func validWindowsFailure(f storage.WindowsFailure) bool {
	switch f {
	case "", storage.WindowsSharingViolation, storage.WindowsLockConflict, storage.WindowsDeletePending, storage.WindowsRangeNotLocked, storage.WindowsNotReparsePoint:
		return true
	}
	return false
}
func windowsActionOf(result storage.WindowsActionResult, reference string) (*windowsAction, error) {
	errno, err := windowsErrnoName(result.Errno)
	if err != nil {
		return nil, err
	}
	value := &windowsAction{Action: result.Action, State: result.State, File: reference, CreateAction: result.CreateAction, Errno: errno, Failure: result.Failure, Applied: result.Applied, HistoryRemaining: result.HistoryRemaining}
	if result.Symlink != nil {
		value.Symlink = &windowsSymlink{Target: result.Symlink.Target, Location: result.Symlink.Location, Unparsed: result.Symlink.Unparsed}
	}
	if result.Attr.ID != 0 {
		value.Attr = windowsAttrOf(result.Attr)
	}
	if err := validateWindowsAction(*value); err != nil {
		return nil, err
	}
	return value, nil
}
func validateWindowsAction(a windowsAction) error {
	if _, err := a.Action.Epoch(); err != nil {
		return err
	}
	if a.State < storage.WindowsActionPending || a.State > storage.WindowsActionCancelled || a.Applied < 0 || a.HistoryRemaining < 0 || !validWindowsFailure(a.Failure) {
		return errors.New("invalid Windows action receipt")
	}
	errno, err := windowsErrno(a.Errno)
	if err != nil {
		return err
	}
	if a.State == storage.WindowsActionRejected && errno == 0 || a.Failure != "" && errno == 0 {
		return errors.New("Windows action failure has no errno")
	}
	if a.File != "" && (!validFileCapability(a.File) || a.Attr == nil || a.CreateAction < storage.WindowsOpened || a.CreateAction > storage.WindowsSuperseded) {
		return errors.New("Windows open receipt has no matching reference attributes")
	}
	if a.Symlink != nil {
		if errno != syscall.ELOOP || a.Failure != "" {
			return errors.New("Windows action has inconsistent symlink classification")
		}
		if err := validateWindowsSymlink(a.Symlink); err != nil {
			return err
		}
	}
	if a.Attr != nil {
		return validateWindowsAttr(a.Attr)
	}
	return nil
}
func validateWindowsBasicAttr(a *windowsBasicAttr) error {
	if a == nil || a.Attr == nil || a.Attr.ID == 0 || a.Attr.Size < 0 || a.CreationTime.Nanos < 0 || a.CreationTime.Nanos >= 1e9 || a.ChangeTime.Nanos < 0 || a.ChangeTime.Nanos >= 1e9 {
		return errors.New("invalid Windows attributes")
	}
	return nil
}
func validateWindowsAttr(a *windowsAttr) error {
	if a == nil {
		return errors.New("missing Windows attributes")
	}
	if err := validateWindowsBasicAttr(a.Basic); err != nil {
		return err
	}
	return a.NameInfo.Check()
}

func validateWindowsRequest(r windowsRequest, maximum storage.FileSessionOptions) error {
	expected := windowsRequest{Op: r.Op, Session: r.Session, Action: r.Action, Data: []byte{}, Ranges: []storage.WindowsLockRange{}}
	switch r.Op {
	case storage.OpWindowsState, storage.OpWindowsEnable, storage.OpWindowsQueryActivation, storage.OpWindowsSessionOpen:
		if r.Session != "" {
			return errors.New("Windows volume operation carries a session")
		}
	default:
		if !validFileCapability(r.Session) {
			return errors.New("invalid Windows session capability")
		}
	}
	if windowsActionRequired(r.Op) {
		if _, err := r.Action.Epoch(); err != nil {
			return err
		}
	} else if r.Action != "" {
		return errors.New("Windows operation does not accept an action")
	}
	switch r.Op {
	case storage.OpWindowsState, storage.OpWindowsEnable, storage.OpWindowsQueryActivation, storage.OpWindowsSessionClose, storage.OpWindowsRenew, storage.OpWindowsStatus, storage.OpWindowsQueryAction, storage.OpWindowsCancelAction:
	case storage.OpWindowsSessionOpen:
		expected.Options = r.Options
		if r.Options == nil {
			return errors.New("Windows session options are missing")
		}
		if err := checkFileSessionOptions(*r.Options, maximum); err != nil {
			return err
		}
	case storage.OpWindowsOpen:
		expected.Open = r.Open
		if r.Open == nil {
			return errors.New("Windows open intent is missing")
		}
		if err := r.Open.storage().Check(); err != nil {
			return err
		}
	case storage.OpWindowsStat, storage.OpWindowsList, storage.OpWindowsReadLink, storage.OpWindowsSync, storage.OpWindowsClose:
		expected.File = r.File
	case storage.OpWindowsRead:
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Length = r.Length
		if err := storage.CheckWindowsRange(r.Offset, int64(r.Length)); err != nil {
			return err
		}
	case storage.OpWindowsWrite:
		expected.File = r.File
		expected.Offset = r.Offset
		expected.Data = r.Data
		if err := storage.CheckWindowsRange(r.Offset, int64(len(r.Data))); err != nil {
			return err
		}
	case storage.OpWindowsTruncate:
		expected.File = r.File
		expected.Offset = r.Offset
		if err := storage.CheckWindowsRange(0, r.Offset); err != nil {
			return err
		}
	case storage.OpWindowsSetLink:
		expected.File = r.File
		expected.Target = r.Target
		if err := storage.CheckWindowsLinkTarget(r.Target); err != nil {
			return err
		}
	case storage.OpWindowsSetAttr:
		expected.File = r.File
		expected.Change = r.Change
		if r.Change == nil || r.Change.Change == nil {
			return errors.New("Windows attribute change is missing")
		}
		if err := r.Change.storage().Check(); err != nil {
			return err
		}
	case storage.OpWindowsRename:
		expected.File = r.File
		expected.Rename = r.Rename
		if r.Rename == nil {
			return errors.New("Windows rename operands are missing")
		}
		if err := r.Rename.Check(); err != nil {
			return err
		}
	case storage.OpWindowsSetDeletePending:
		expected.File = r.File
		expected.DeletePending = r.DeletePending
	case storage.OpWindowsLockBatch:
		expected.File = r.File
		expected.Ranges = r.Ranges
		if err := (storage.WindowsLockBatch{Ranges: r.Ranges}).Check(); err != nil {
			return err
		}
	default:
		return errors.New("unknown Windows operation")
	}
	if expected.File != "" && !validFileCapability(expected.File) {
		return errors.New("invalid Windows file capability")
	}
	switch r.Op {
	case storage.OpWindowsStat, storage.OpWindowsList, storage.OpWindowsReadLink, storage.OpWindowsSetLink, storage.OpWindowsSync, storage.OpWindowsClose, storage.OpWindowsRead, storage.OpWindowsWrite, storage.OpWindowsTruncate, storage.OpWindowsSetAttr, storage.OpWindowsRename, storage.OpWindowsSetDeletePending, storage.OpWindowsLockBatch:
		if expected.File == "" {
			return errors.New("Windows file operation has no reference")
		}
	}
	if !reflect.DeepEqual(r, expected) {
		return fmt.Errorf("Windows operation %s carries unrelated operands", r.Op)
	}
	return nil
}

func validateWindowsResponse(req windowsRequest, r windowsResponse) error {
	expected := windowsResponse{Data: []byte{}, Entries: []windowsEntry{}}
	switch req.Op {
	case storage.OpWindowsState:
		expected.State = r.State
		if err := validateWindowsState(r.State); err != nil {
			return err
		}
	case storage.OpWindowsEnable, storage.OpWindowsQueryActivation:
		expected.Activation = r.Activation
		if err := validateWindowsActivation(req.Action, r.Activation); err != nil {
			return err
		}
	case storage.OpWindowsSessionOpen:
		expected.Session = r.Session
		expected.Status = r.Status
		if !validFileCapability(r.Session) || r.Status == nil || r.Status.Retired || r.Status.Remaining <= 0 {
			return errors.New("Windows enrollment has no live session")
		}
	case storage.OpWindowsStatus, storage.OpWindowsRenew:
		expected.Status = r.Status
		if r.Status == nil {
			return errors.New("Windows session status is missing")
		}
	case storage.OpWindowsOpen:
		expected.File = r.File
		expected.Attr = r.Attr
		expected.CreateAction = r.CreateAction
		if !validFileCapability(r.File) || r.CreateAction < storage.WindowsOpened || r.CreateAction > storage.WindowsSuperseded {
			return errors.New("Windows open has no valid reference or disposition outcome")
		}
		if err := validateWindowsAttr(r.Attr); err != nil {
			return err
		}
	case storage.OpWindowsStat:
		expected.Attr = r.Attr
		if err := validateWindowsAttr(r.Attr); err != nil {
			return err
		}
	case storage.OpWindowsRead:
		expected.ReadAttr = r.ReadAttr
		expected.Data = r.Data
		if r.ReadAttr == nil || r.ReadAttr.ID == 0 || r.ReadAttr.Size < 0 || len(r.Data) > req.Length || int64(len(r.Data)) > max(r.ReadAttr.Size-req.Offset, 0) {
			return errors.New("Windows read bytes disagree with its captured attributes")
		}
		if req.Length > 0 && req.Offset < r.ReadAttr.Size && len(r.Data) == 0 {
			return errors.New("Windows read made no progress before EOF")
		}
	case storage.OpWindowsReadLink:
		expected.Symlink = r.Symlink
		if err := validateWindowsSymlink(r.Symlink); err != nil {
			return err
		}
		if r.Symlink.Unparsed != "" {
			return errors.New("read-link response carries an unresolved suffix")
		}
	case storage.OpWindowsList:
		expected.Entries = r.Entries
		previous := ""
		for index, entry := range r.Entries {
			if entry.Name == "" || entry.Name == "." || entry.Name == ".." || strings.ContainsAny(entry.Name, "/\\\x00") || !utf8.ValidString(entry.Name) || index > 0 && entry.Name <= previous {
				return errors.New("Windows listing has invalid or unordered names")
			}
			if err := validateWindowsBasicAttr(entry.Attr); err != nil {
				return err
			}
			previous = entry.Name
		}
	case storage.OpWindowsSessionClose, storage.OpWindowsSync:
	case storage.OpWindowsWrite, storage.OpWindowsTruncate, storage.OpWindowsSetAttr, storage.OpWindowsRename, storage.OpWindowsSetDeletePending, storage.OpWindowsLockBatch, storage.OpWindowsClose, storage.OpWindowsSetLink, storage.OpWindowsQueryAction, storage.OpWindowsCancelAction:
		expected.Action = r.Action
		if r.Action == nil || r.Action.Action != req.Action {
			return errors.New("Windows action response does not match its request")
		}
		if err := validateWindowsAction(*r.Action); err != nil {
			return err
		}
		if err := validateWindowsBatchAction(req, r.Action); err != nil {
			return err
		}
	default:
		return errors.New("unknown Windows response operation")
	}
	if r.Status != nil && (r.Status.Epoch == "" || len(r.Status.Epoch) > MaxLockCapabilityBytes || r.Status.ActionEpoch == 0 || r.Status.Revision == 0 || r.Status.Remaining < 0 || r.Status.HistoryRemaining < 0) {
		return errors.New("Windows status has invalid lifetime evidence")
	}
	if !reflect.DeepEqual(r, expected) {
		return errors.New("Windows response carries unrelated results")
	}
	return nil
}

func validateWindowsActivation(id storage.WindowsActionID, a *windowsActivation) error {
	if a == nil || a.Action != id || a.State < storage.WindowsActionPending || a.State > storage.WindowsActionCancelled || a.HistoryRemaining < 0 {
		return errors.New("Windows activation receipt is invalid")
	}
	errno, err := windowsErrno(a.Errno)
	if err != nil {
		return err
	}
	if a.State == storage.WindowsActionRejected && errno == 0 {
		return errors.New("rejected Windows activation has no error")
	}
	return nil
}

func validateWindowsSymlink(link *windowsSymlink) error {
	if link == nil {
		return errors.New("missing Windows symlink detail")
	}
	if err := storage.CheckWindowsLinkTarget(link.Target); err != nil {
		return err
	}
	if link.Location.State != storage.WindowsNameLinked || link.Location.Check() != nil {
		return errors.New("invalid authoritative symlink location")
	}
	if len(link.Unparsed) > storage.WindowsMaxLinkTargetBytes {
		return errors.New("Windows symlink suffix exceeds its bound")
	}
	if link.Unparsed != "" {
		if !strings.HasPrefix(link.Unparsed, "/") {
			return errors.New("Windows symlink suffix is not an unresolved slash path")
		}
		if err := (storage.WindowsNameInfo{State: storage.WindowsNameLinked, Path: link.Unparsed[1:]}).Check(); err != nil {
			return err
		}
	}
	return nil
}

func validateWindowsBatchAction(req windowsRequest, action *windowsAction) error {
	if req.Op != storage.OpWindowsLockBatch || action == nil {
		return nil
	}
	if action.Applied > len(req.Ranges) {
		return errors.New("Windows lock batch receipt exceeds its requested element count")
	}
	if action.State == storage.WindowsActionCompleted && action.Errno == "" && action.Applied != len(req.Ranges) {
		return errors.New("Windows lock batch success did not apply every requested element")
	}
	return nil
}

func validateWindowsState(state *storage.WindowsState) error {
	if state == nil || state.ActionEpoch == 0 || state.MaxEventBytes <= 0 || state.VolumeIdentity == "" || len(state.VolumeIdentity) > MaxLockCapabilityBytes || !utf8.ValidString(state.VolumeIdentity) {
		return errors.New("Windows capability response is incomplete or exceeds its identity bound")
	}
	return nil
}
