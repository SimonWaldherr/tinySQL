package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// App contains HTTP dependencies.
type App struct {
	store            *Store
	auth             *AuthService
	tpl              *template.Template
	defaultAdminUser string
	defaultAdminPass string
	adminHintActive  atomic.Bool
}

// NewApp creates an application instance.
func NewApp(store *Store, auth *AuthService, tpl *template.Template, defaultAdminUser, defaultAdminPass string) *App {
	return &App{store: store, auth: auth, tpl: tpl, defaultAdminUser: defaultAdminUser, defaultAdminPass: defaultAdminPass}
}

// RegisterRoutes registers all HTML and JSON routes.
func (a *App) RegisterRoutes(mux *http.ServeMux) {
	anyAuth := a.auth.RequireRole(RoleAdmin, RoleEditor, RoleViewer, RoleUser)
	canEdit := a.auth.RequireRole(RoleAdmin, RoleEditor)
	canViewAnswers := a.auth.RequireRole(RoleAdmin, RoleEditor, RoleViewer)
	canSubmit := a.auth.RequireRole(RoleAdmin, RoleEditor, RoleUser)
	adminOnly := a.auth.RequireRole(RoleAdmin)

	mux.HandleFunc("GET /login", a.loginPageHandler)
	mux.HandleFunc("POST /login", a.loginHandler)
	mux.HandleFunc("POST /logout", anyAuth(a.logoutHandler))

	mux.HandleFunc("GET /", anyAuth(a.listFormsHandler))
	mux.HandleFunc("GET /forms/new", canEdit(a.newFormHandler))
	mux.HandleFunc("POST /forms", canEdit(a.createFormHandler))
	mux.HandleFunc("GET /forms/{id}/fill", canSubmit(a.answerFormHandler))
	mux.HandleFunc("POST /forms/{id}/answers", canSubmit(a.saveAnswerHandler))
	mux.HandleFunc("GET /forms/{id}/answers", canViewAnswers(a.listAnswersHandler))

	mux.HandleFunc("GET /users", adminOnly(a.listUsersHandler))
	mux.HandleFunc("GET /users/new", adminOnly(a.newUserHandler))
	mux.HandleFunc("POST /users", adminOnly(a.createUserHandler))

	// Public guest routes (no auth required).
	mux.HandleFunc("GET /g/{id}", a.guestFillHandler)
	mux.HandleFunc("POST /g/{id}", a.guestSubmitHandler)

	// Profile / password change.
	mux.HandleFunc("GET /profile", anyAuth(a.profileHandler))
	mux.HandleFunc("POST /profile/password", anyAuth(a.changePasswordHandler))

	mux.HandleFunc("GET /api/forms", anyAuth(a.apiFormsHandler))
	mux.HandleFunc("GET /api/forms/{id}/answers", canViewAnswers(a.apiAnswersHandler))
	mux.HandleFunc("POST /api/forms/{id}/answers", canSubmit(a.apiSubmitAnswerHandler))
}

// loginPageHandler renders the login page.
func (a *App) loginPageHandler(w http.ResponseWriter, r *http.Request) {
	if CurrentUser(r) != nil {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	a.render(w, r, "login", map[string]any{})
}

// loginHandler authenticates a user.
func (a *App) loginHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.render(w, r, "login", map[string]any{"Error": "Ungültige Anmeldung."})
		return
	}
	if err := a.auth.Login(w, r, r.Form.Get("username"), r.Form.Get("password")); err != nil {
		a.render(w, r, "login", map[string]any{"Error": "Benutzername oder Passwort ist falsch."})
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// logoutHandler deletes the current session.
func (a *App) logoutHandler(w http.ResponseWriter, r *http.Request) {
	a.auth.Logout(w, r)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// listFormsHandler renders all forms.
func (a *App) listFormsHandler(w http.ResponseWriter, r *http.Request) {
	forms, err := a.store.ListForms(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	a.render(w, r, "form_list", map[string]any{"Forms": forms})
}

// newFormHandler renders the form builder.
func (a *App) newFormHandler(w http.ResponseWriter, r *http.Request) {
	a.render(w, r, "form_new", map[string]any{})
}

// createFormHandler stores a new form definition.
func (a *App) createFormHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.badRequest(w, "Ungültige Formulardaten.")
		return
	}
	input, err := parseCreateForm(r.Form, CurrentUser(r).ID)
	if err != nil {
		a.render(w, r, "form_new", map[string]any{"Error": err.Error()})
		return
	}
	id, err := a.store.CreateForm(r.Context(), input)
	if err != nil {
		a.serverError(w, err)
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/forms/%d/fill", id), http.StatusSeeOther)
}

// answerFormHandler renders a form for user input.
func (a *App) answerFormHandler(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	detail, err := a.store.GetFormDetail(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	actionURL := fmt.Sprintf("/forms/%d/answers", id)
	data := map[string]any{"Detail": detail, "Form": detail.Form, "Fields": detail.Fields, "ActionURL": actionURL}
	if open, msg := formTimeCheck(detail.Form); !open {
		data["Closed"] = msg
	}
	a.render(w, r, "form_answer", data)
}

// saveAnswerHandler stores a submitted answer.
func (a *App) saveAnswerHandler(w http.ResponseWriter, r *http.Request) {
	formID, err := pathID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	detail, err := a.store.GetFormDetail(r.Context(), formID)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	actionURL := fmt.Sprintf("/forms/%d/answers", formID)
	if open, msg := formTimeCheck(detail.Form); !open {
		a.render(w, r, "form_answer", map[string]any{"Detail": detail, "Form": detail.Form, "Fields": detail.Fields, "Closed": msg, "ActionURL": actionURL})
		return
	}
	if err := r.ParseForm(); err != nil {
		a.badRequest(w, "Ungültige Formulardaten.")
		return
	}
	user := CurrentUser(r)
	input := SaveAnswerInput{
		FormID:        formID,
		UserID:        user.ID,
		SubmitterName: strings.TrimSpace(r.Form.Get("submitter")),
		Values:        parseAnswerValues(r.Form),
	}
	if input.SubmitterName == "" {
		input.SubmitterName = user.DisplayName
	}
	if _, err := a.store.SaveAnswer(r.Context(), input); err != nil {
		a.render(w, r, "form_answer", map[string]any{"Detail": detail, "Form": detail.Form, "Fields": detail.Fields, "Error": err.Error(), "ActionURL": actionURL})
		return
	}
	http.Redirect(w, r, fmt.Sprintf("/forms/%d/answers", formID), http.StatusSeeOther)
}

// listAnswersHandler renders all answers for a form.
func (a *App) listAnswersHandler(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	detail, answers, err := a.store.ListAnswers(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	a.render(w, r, "answer_list", map[string]any{"Form": detail.Form, "Fields": detail.Fields, "Answers": answers})
}

// listUsersHandler renders the user administration page.
func (a *App) listUsersHandler(w http.ResponseWriter, r *http.Request) {
	users, err := a.store.ListUsers(r.Context())
	if err != nil {
		a.serverError(w, err)
		return
	}
	a.render(w, r, "user_list", map[string]any{"Users": users})
}

// newUserHandler renders the user creation page.
func (a *App) newUserHandler(w http.ResponseWriter, r *http.Request) {
	a.render(w, r, "user_new", map[string]any{"Roles": []Role{RoleAdmin, RoleEditor, RoleViewer, RoleUser}})
}

// createUserHandler creates a user with a role.
func (a *App) createUserHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.badRequest(w, "Ungültige Formulardaten.")
		return
	}
	username := strings.TrimSpace(r.Form.Get("username"))
	displayName := strings.TrimSpace(r.Form.Get("display_name"))
	password := r.Form.Get("password")
	role := Role(r.Form.Get("role"))
	if username == "" || displayName == "" || password == "" || !roleAllowed(role, RoleAdmin, RoleEditor, RoleViewer, RoleUser) {
		a.render(w, r, "user_new", map[string]any{"Roles": []Role{RoleAdmin, RoleEditor, RoleViewer, RoleUser}, "Error": "Bitte alle Felder korrekt ausfüllen."})
		return
	}
	hash, err := HashPassword(password)
	if err != nil {
		a.serverError(w, err)
		return
	}
	if _, err := a.store.CreateUser(r.Context(), username, displayName, hash, role, true); err != nil {
		a.render(w, r, "user_new", map[string]any{"Roles": []Role{RoleAdmin, RoleEditor, RoleViewer, RoleUser}, "Error": err.Error()})
		return
	}
	http.Redirect(w, r, "/users", http.StatusSeeOther)
}

// apiFormsHandler returns all forms as JSON.
func (a *App) apiFormsHandler(w http.ResponseWriter, r *http.Request) {
	forms, err := a.store.ListForms(r.Context())
	if err != nil {
		a.jsonError(w, http.StatusInternalServerError, "database error")
		return
	}
	a.writeJSON(w, http.StatusOK, forms)
}

// apiAnswersHandler returns all answers for a form as JSON.
func (a *App) apiAnswersHandler(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		a.jsonError(w, http.StatusNotFound, "not found")
		return
	}
	detail, answers, err := a.store.ListAnswers(r.Context(), id)
	if err != nil {
		a.jsonError(w, http.StatusInternalServerError, "database error")
		return
	}
	a.writeJSON(w, http.StatusOK, map[string]any{"form": detail.Form, "fields": detail.Fields, "answers": answers})
}

// apiSubmitAnswerHandler stores a JSON answer submission.
func (a *App) apiSubmitAnswerHandler(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		a.jsonError(w, http.StatusNotFound, "not found")
		return
	}
	var payload struct {
		SubmitterName string            `json:"submitter_name"`
		Values        map[string]string `json:"values"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		a.jsonError(w, http.StatusBadRequest, "invalid json")
		return
	}
	values := make(map[int64]string, len(payload.Values))
	for rawID, value := range payload.Values {
		fieldID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			a.jsonError(w, http.StatusBadRequest, "invalid field id")
			return
		}
		values[fieldID] = value
	}
	user := CurrentUser(r)
	if payload.SubmitterName == "" {
		payload.SubmitterName = user.DisplayName
	}
	submissionID, err := a.store.SaveAnswer(r.Context(), SaveAnswerInput{FormID: id, UserID: user.ID, SubmitterName: payload.SubmitterName, Values: values})
	if err != nil {
		a.jsonError(w, http.StatusBadRequest, err.Error())
		return
	}
	a.writeJSON(w, http.StatusCreated, map[string]any{"id": submissionID})
}

// render executes a named template with common request data.
func (a *App) render(w http.ResponseWriter, r *http.Request, name string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	rc := CurrentRequestContext(r)
	data["CurrentUser"] = rc.User
	data["CSRFToken"] = rc.CSRFToken
	data["AdminHintActive"] = a.adminHintActive.Load()
	data["DefaultAdminUser"] = a.defaultAdminUser
	data["DefaultAdminPass"] = a.defaultAdminPass
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := a.tpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, "template error", http.StatusInternalServerError)
	}
}

// serverError writes a generic server error.
func (a *App) serverError(w http.ResponseWriter, err error) {
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

// badRequest writes a plain bad request response.
func (a *App) badRequest(w http.ResponseWriter, msg string) {
	http.Error(w, msg, http.StatusBadRequest)
}

// writeJSON writes a JSON response.
func (a *App) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// jsonError writes a JSON error response.
func (a *App) jsonError(w http.ResponseWriter, status int, msg string) {
	a.writeJSON(w, status, map[string]string{"error": msg})
}

// pathID parses an integer path variable named id.
func pathID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

// formTimeCheck returns whether the form is currently accepting submissions.
func formTimeCheck(f Form) (open bool, msg string) {
	now := time.Now().Format("2006-01-02T15:04")
	if f.OpensAt != "" && now < f.OpensAt {
		t, _ := time.Parse("2006-01-02T15:04", f.OpensAt)
		return false, "Das Formular öffnet am " + t.Format("02.01.2006 um 15:04 Uhr") + "."
	}
	if f.ClosesAt != "" && now > f.ClosesAt {
		t, _ := time.Parse("2006-01-02T15:04", f.ClosesAt)
		return false, "Das Formular ist seit " + t.Format("02.01.2006 um 15:04 Uhr") + " geschlossen."
	}
	return true, ""
}

// guestFillHandler renders a public form for guest users.
func (a *App) guestFillHandler(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	detail, err := a.store.GetFormDetail(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) || (!errors.Is(err, nil) && err != nil) {
		http.NotFound(w, r)
		return
	}
	if err != nil {
		a.serverError(w, err)
		return
	}
	if !detail.Form.AllowGuest {
		http.Error(w, "Dieses Formular ist nicht öffentlich zugänglich.", http.StatusForbidden)
		return
	}
	actionURL := fmt.Sprintf("/g/%d", id)
	data := map[string]any{"Detail": detail, "Form": detail.Form, "Fields": detail.Fields, "ActionURL": actionURL, "IsGuest": true}
	if open, msg := formTimeCheck(detail.Form); !open {
		data["Closed"] = msg
	}
	a.render(w, r, "form_answer", data)
}

// guestSubmitHandler stores a submission from a guest user.
func (a *App) guestSubmitHandler(w http.ResponseWriter, r *http.Request) {
	formID, err := pathID(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	detail, err := a.store.GetFormDetail(r.Context(), formID)
	if err != nil || !detail.Form.AllowGuest {
		http.Error(w, "Formular nicht gefunden oder nicht öffentlich.", http.StatusForbidden)
		return
	}
	actionURL := fmt.Sprintf("/g/%d", formID)
	if open, msg := formTimeCheck(detail.Form); !open {
		a.render(w, r, "form_answer", map[string]any{"Detail": detail, "Form": detail.Form, "Fields": detail.Fields, "Closed": msg, "ActionURL": actionURL, "IsGuest": true})
		return
	}
	if err := r.ParseForm(); err != nil {
		a.badRequest(w, "Ungültige Formulardaten.")
		return
	}
	submitter := strings.TrimSpace(r.Form.Get("submitter"))
	if submitter == "" {
		submitter = "Gast"
	}
	if _, err := a.store.SaveAnswer(r.Context(), SaveAnswerInput{
		FormID:        formID,
		UserID:        0,
		SubmitterName: submitter,
		Values:        parseAnswerValues(r.Form),
	}); err != nil {
		a.render(w, r, "form_answer", map[string]any{"Detail": detail, "Form": detail.Form, "Fields": detail.Fields, "Error": err.Error(), "ActionURL": actionURL, "IsGuest": true})
		return
	}
	a.render(w, r, "guest_thanks", map[string]any{"Form": detail.Form})
}

// profileHandler renders the password-change profile page.
func (a *App) profileHandler(w http.ResponseWriter, r *http.Request) {
	a.render(w, r, "profile", map[string]any{})
}

// changePasswordHandler updates the current user's password.
func (a *App) changePasswordHandler(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		a.badRequest(w, "Ungültige Formulardaten.")
		return
	}
	user := CurrentUser(r)
	oldPass := r.Form.Get("old_password")
	newPass := r.Form.Get("new_password")
	confirm := r.Form.Get("confirm_password")

	if !CheckPassword(user.PasswordHash, oldPass) {
		a.render(w, r, "profile", map[string]any{"Error": "Aktuelles Passwort ist falsch."})
		return
	}
	if len(newPass) < 8 {
		a.render(w, r, "profile", map[string]any{"Error": "Das neue Passwort muss mindestens 8 Zeichen lang sein."})
		return
	}
	if newPass != confirm {
		a.render(w, r, "profile", map[string]any{"Error": "Die Passwörter stimmen nicht überein."})
		return
	}
	hash, err := HashPassword(newPass)
	if err != nil {
		a.serverError(w, err)
		return
	}
	if err := a.store.UpdateUserPassword(r.Context(), user.ID, hash); err != nil {
		a.serverError(w, err)
		return
	}
	if user.Username == a.defaultAdminUser {
		a.adminHintActive.Store(false)
	}
	a.render(w, r, "profile", map[string]any{"Success": "Passwort erfolgreich geändert."})
}
